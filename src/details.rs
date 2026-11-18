//! A relation's properties, partitions and dependencies (SpelaionDB's SDB-393), from
//! PostgreSQL's catalog.
//!
//! Dependencies are what `pg_depend` records: the relations and functions a view's rule reads,
//! foreign keys both ways, the sequences a table's columns take their values from, and the
//! triggers on it. What a function's body touches is not recorded (PL/pgSQL is parsed only when
//! it runs), so functions are among the untracked kinds.
//!
//! CockroachDB speaks this protocol with a catalog of its own: a query it cannot answer (a
//! table or function it does not have) means it does not tell, never an error.

use spelaion_driver_api::{
    Dependencies, DependencyInfo, DependencyKind, DriverError, ObjectProperty, PartitionInfo,
    Partitioning, Result, property,
};
use tokio_postgres::Client;

use crate::map_error;

/// A catalog this server does not have (CockroachDB): undefined table, column or function, or
/// a feature it does not support.
fn not_here(error: &tokio_postgres::Error) -> bool {
    error
        .code()
        .is_some_and(|code| matches!(code.code(), "42P01" | "42703" | "42883" | "0A000"))
}

fn missing(schema: &str, name: &str) -> DriverError {
    DriverError::Other(format!("there is no relation {schema}.{name}"))
}

/// The relation's oid, by its schema and name as the catalog writes them.
const RELATION: &str = "select c.oid from pg_class c join pg_namespace n on n.oid = c.relnamespace \
                        where n.nspname = $1 and c.relname = $2";

pub async fn properties(client: &Client, schema: &str, name: &str) -> Result<Vec<ObjectProperty>> {
    let full = client
        .query_opt(
            "select pg_get_userbyid(c.relowner)::text,
                    coalesce(t.spcname::text, (select d.spcname::text from pg_database db
                                               join pg_tablespace d on d.oid = db.dattablespace
                                               where db.datname = current_database())),
                    am.amname::text,
                    case c.relpersistence when 'p' then 'permanent' when 'u' then 'unlogged'
                                          when 't' then 'temporary' end,
                    array_to_string(c.reloptions, ', ')
             from pg_class c
             join pg_namespace n on n.oid = c.relnamespace
             left join pg_tablespace t on t.oid = c.reltablespace
             left join pg_am am on am.oid = c.relam
             where n.nspname = $1 and c.relname = $2",
            &[&schema, &name],
        )
        .await;
    let row = match full {
        Ok(row) => row,
        // A catalog without access methods or tablespaces: the owner is what it tells.
        Err(e) if not_here(&e) => client
            .query_opt(
                "select pg_get_userbyid(c.relowner)::text, null::text, null::text, null::text,
                        null::text
                 from pg_class c join pg_namespace n on n.oid = c.relnamespace
                 where n.nspname = $1 and c.relname = $2",
                &[&schema, &name],
            )
            .await
            .map_err(map_error)?,
        Err(e) => return Err(map_error(e)),
    };
    let row = row.ok_or_else(|| missing(schema, name))?;
    let mut out = Vec::new();
    for (i, key) in [
        property::OWNER,
        property::TABLESPACE,
        property::ENGINE,
        property::PERSISTENCE,
        property::OPTIONS,
    ]
    .into_iter()
    .enumerate()
    {
        let value: Option<String> = row.get(i);
        if let Some(value) = value.filter(|v| !v.is_empty()) {
            out.push(ObjectProperty {
                name: key.into(),
                value,
            });
        }
    }
    Ok(out)
}

/// `RANGE (created_at)` → `created_at`.
fn partition_key(definition: &str) -> String {
    let inner = definition
        .find('(')
        .map(|open| &definition[open + 1..])
        .unwrap_or(definition);
    inner.strip_suffix(')').unwrap_or(inner).trim().to_string()
}

pub async fn partitions(client: &Client, schema: &str, name: &str) -> Result<Option<Partitioning>> {
    let scheme = client
        .query_opt(
            "select pt.partstrat::text, pg_get_partkeydef(c.oid)
             from pg_partitioned_table pt
             join pg_class c on c.oid = pt.partrelid
             join pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relname = $2",
            &[&schema, &name],
        )
        .await;
    let scheme = match scheme {
        Ok(Some(row)) => row,
        Ok(None) => return Ok(None),
        Err(e) if not_here(&e) => return Ok(None),
        Err(e) => return Err(map_error(e)),
    };
    let strategy: String = scheme.get(0);
    let definition: String = scheme.get(1);
    let method = match strategy.as_str() {
        "r" => "range",
        "l" => "list",
        "h" => "hash",
        other => other,
    }
    .to_string();
    let rows = client
        .query(
            &format!(
                "select pn.nspname::text, p.relname::text, pg_get_expr(p.relpartbound, p.oid),
                        case when p.reltuples < 0 then null else p.reltuples::int8 end,
                        p.relkind = 'p'
                 from pg_inherits i
                 join pg_class p on p.oid = i.inhrelid
                 join pg_namespace pn on pn.oid = p.relnamespace
                 where i.inhparent = ({RELATION})
                 order by pg_get_expr(p.relpartbound, p.oid) = 'DEFAULT', p.relname"
            ),
            &[&schema, &name],
        )
        .await
        .map_err(map_error)?;
    Ok(Some(Partitioning {
        method,
        key: partition_key(&definition),
        partitions: rows
            .iter()
            .map(|r| PartitionInfo {
                schema: r.get(0),
                name: r.get(1),
                bound: r.get(2),
                estimated_rows: r.get(3),
                subpartitioned: r.get(4),
            })
            .collect(),
    }))
}

fn relation_kind(relkind: &str) -> DependencyKind {
    match relkind {
        "v" => DependencyKind::View,
        "m" => DependencyKind::MaterializedView,
        "S" => DependencyKind::Sequence,
        "i" | "I" => DependencyKind::Index,
        "r" | "p" | "f" => DependencyKind::Table,
        _ => DependencyKind::Other,
    }
}

fn kind_order(kind: DependencyKind) -> u8 {
    match kind {
        DependencyKind::Table => 0,
        DependencyKind::View => 1,
        DependencyKind::MaterializedView => 2,
        DependencyKind::ForeignKey => 3,
        DependencyKind::Trigger => 4,
        DependencyKind::Function => 5,
        DependencyKind::Procedure => 6,
        DependencyKind::Sequence => 7,
        DependencyKind::Type => 8,
        DependencyKind::Index => 9,
        DependencyKind::Other => 10,
    }
}

fn sorted(mut list: Vec<DependencyInfo>) -> Vec<DependencyInfo> {
    list.sort_by(|a, b| {
        (kind_order(a.kind), &a.schema, &a.name, &a.detail).cmp(&(
            kind_order(b.kind),
            &b.schema,
            &b.name,
            &b.detail,
        ))
    });
    list.dedup();
    list
}

/// Rows of (schema, name, relkind or `proc`, detail) as dependencies.
fn infos(rows: &[tokio_postgres::Row]) -> Vec<DependencyInfo> {
    rows.iter()
        .map(|r| {
            let kind: String = r.get(2);
            DependencyInfo {
                schema: r.get(0),
                name: r.get(1),
                kind: match kind.as_str() {
                    "proc" => DependencyKind::Function,
                    "fk" => DependencyKind::ForeignKey,
                    "trigger" => DependencyKind::Trigger,
                    other => relation_kind(other),
                },
                detail: r.get(3),
            }
        })
        .collect()
}

/// What the relation reads or points to.
const USES: &str = "
    with target as ({RELATION})
    -- What a view's rule reads: relations and functions.
    select distinct rn.nspname::text, rc.relname::text, rc.relkind::text, null::text
    from pg_rewrite r
    join pg_depend d on d.classid = 'pg_rewrite'::regclass and d.objid = r.oid
                    and d.refclassid = 'pg_class'::regclass
    join pg_class rc on rc.oid = d.refobjid
    join pg_namespace rn on rn.oid = rc.relnamespace
    where r.ev_class = (select oid from target) and d.refobjid <> (select oid from target)
    union
    select distinct pn.nspname::text, p.proname::text, 'proc', null::text
    from pg_rewrite r
    join pg_depend d on d.classid = 'pg_rewrite'::regclass and d.objid = r.oid
                    and d.refclassid = 'pg_proc'::regclass
    join pg_proc p on p.oid = d.refobjid
    join pg_namespace pn on pn.oid = p.pronamespace
    where r.ev_class = (select oid from target) and pn.nspname <> 'pg_catalog'
    union
    -- The tables its foreign keys reference.
    select fn.nspname::text, f.relname::text, f.relkind::text,
           con.conname::text || ' ' || pg_get_constraintdef(con.oid)
    from pg_constraint con
    join pg_class f on f.oid = con.confrelid
    join pg_namespace fn on fn.oid = f.relnamespace
    where con.contype = 'f' and con.conrelid = (select oid from target) and con.conparentid = 0
    union
    -- The sequences its columns take their values from (a default, serial or identity).
    select sn.nspname::text, s.relname::text, 'S', a.attname::text
    from pg_depend d
    join pg_class s on s.oid = d.objid and s.relkind = 'S'
    join pg_namespace sn on sn.oid = s.relnamespace
    left join pg_attribute a on a.attrelid = d.refobjid and a.attnum = d.refobjsubid
    where d.classid = 'pg_class'::regclass and d.refclassid = 'pg_class'::regclass
      and d.refobjid = (select oid from target) and d.deptype in ('a', 'i')
    union
    select sn.nspname::text, s.relname::text, 'S', a.attname::text
    from pg_attrdef ad
    join pg_depend d on d.classid = 'pg_attrdef'::regclass and d.objid = ad.oid
                    and d.refclassid = 'pg_class'::regclass
    join pg_class s on s.oid = d.refobjid and s.relkind = 'S'
    join pg_namespace sn on sn.oid = s.relnamespace
    join pg_attribute a on a.attrelid = ad.adrelid and a.attnum = ad.adnum
    where ad.adrelid = (select oid from target)";

/// What reads the relation or points to it.
const USED_BY: &str = "
    with target as ({RELATION})
    -- Views and materialized views whose rule reads it.
    select distinct vn.nspname::text, v.relname::text, v.relkind::text, null::text
    from pg_depend d
    join pg_rewrite r on d.classid = 'pg_rewrite'::regclass and d.objid = r.oid
    join pg_class v on v.oid = r.ev_class
    join pg_namespace vn on vn.oid = v.relnamespace
    where d.refclassid = 'pg_class'::regclass and d.refobjid = (select oid from target)
      and v.oid <> (select oid from target)
    union
    -- Foreign keys of other tables (or of itself) that point to it.
    select tn.nspname::text, t.relname::text, 'fk',
           con.conname::text || ' ' || pg_get_constraintdef(con.oid)
    from pg_constraint con
    join pg_class t on t.oid = con.conrelid
    join pg_namespace tn on tn.oid = t.relnamespace
    -- A partition's copy of its table's key (`conparentid`) is that key, already listed.
    where con.contype = 'f' and con.confrelid = (select oid from target) and con.conparentid = 0
    union
    -- The triggers on it, and the function each one executes.
    select n.nspname::text, tg.tgname::text, 'trigger',
           'executes ' || pn.nspname || '.' || p.proname || '()'
    from pg_trigger tg
    join pg_class c on c.oid = tg.tgrelid
    join pg_namespace n on n.oid = c.relnamespace
    join pg_proc p on p.oid = tg.tgfoid
    join pg_namespace pn on pn.oid = p.pronamespace
    where tg.tgrelid = (select oid from target) and not tg.tgisinternal";

pub async fn dependencies(
    client: &Client,
    schema: &str,
    name: &str,
) -> Result<Option<Dependencies>> {
    match client.query_opt(RELATION, &[&schema, &name]).await {
        Ok(Some(_)) => {}
        Ok(None) => return Err(missing(schema, name)),
        Err(e) => return Err(map_error(e)),
    }
    let uses = client
        .query(&USES.replace("{RELATION}", RELATION), &[&schema, &name])
        .await;
    let used_by = client
        .query(&USED_BY.replace("{RELATION}", RELATION), &[&schema, &name])
        .await;
    let (uses, used_by) = match (uses, used_by) {
        (Ok(uses), Ok(used_by)) => (uses, used_by),
        (Err(e), _) | (_, Err(e)) if not_here(&e) => return Ok(None),
        (Err(e), _) | (_, Err(e)) => return Err(map_error(e)),
    };
    Ok(Some(Dependencies {
        uses: sorted(infos(&uses)),
        used_by: sorted(infos(&used_by)),
        untracked: vec![DependencyKind::Function],
    }))
}

#[cfg(test)]
mod tests {
    use super::partition_key;

    #[test]
    fn the_partition_key_is_read_from_its_definition() {
        assert_eq!(partition_key("RANGE (created_at)"), "created_at");
        assert_eq!(partition_key("LIST (region, kind)"), "region, kind");
        assert_eq!(
            partition_key("HASH (lower((name)::text))"),
            "lower((name)::text)"
        );
    }
}
