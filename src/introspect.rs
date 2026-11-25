//! PostgreSQL catalog queries for the explorer, autocompletion and the MCP server.

use spelaion_driver_api::{
    ColumnInfo, ConstraintInfo, ConstraintKind, DriverError, ForeignKeyInfo, IndexInfo, LikeFilter,
    ObjectInfo, ObjectKind, ReferencingKey, Result, SchemaColumn, SchemaInfo, TableInfo,
};
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Row};

use crate::map_error;

/// `like` as SQL over `column`, its patterns appended to `params` as `$n` after `before`
/// other parameters (ADR-025).
pub(crate) fn like_sql(
    like: &LikeFilter,
    column: &str,
    params: &mut Vec<String>,
    before: usize,
) -> String {
    like.sql(column, |pattern| {
        params.push(pattern.to_string());
        format!("${}", before + params.len())
    })
}

/// Parameters for `client.query`: the fixed ones first, then the patterns.
pub(crate) fn query_params<'a>(
    fixed: &'a [&'a str],
    patterns: &'a [String],
) -> Vec<&'a (dyn ToSql + Sync)> {
    fixed
        .iter()
        .map(|p| p as &(dyn ToSql + Sync))
        .chain(patterns.iter().map(|p| p as &(dyn ToSql + Sync)))
        .collect()
}

/// The server's databases a session of this user could open (SpelaionDB's ADR-074): those that
/// accept connections, never a template, and only where the user has `CONNECT` (a superuser
/// has it everywhere), ordered by name. A read of the catalog: no name is written into SQL.
pub async fn list_databases(client: &Client) -> Result<Vec<String>> {
    let rows = client
        .query(
            "select datname::text from pg_catalog.pg_database
             where datallowconn and not datistemplate
               and pg_catalog.has_database_privilege(oid, 'CONNECT')
             order by datname",
            &[],
        )
        .await
        .map_err(map_error)?;
    Ok(rows.iter().map(|r| r.get(0)).collect())
}

pub async fn list_schemas(client: &Client, like: &LikeFilter) -> Result<Vec<SchemaInfo>> {
    let mut patterns = Vec::new();
    let narrow = like_sql(like, "nspname::text", &mut patterns, 0);
    let sql = format!(
        r"select nspname::text,
                 nspname in ('pg_catalog', 'information_schema') or nspname like 'pg\_%' as system
          from pg_namespace
          where nspname not like 'pg\_toast%' and nspname not like 'pg\_temp\_%'{narrow}
          order by system, nspname"
    );
    let rows = client
        .query(&sql, &query_params(&[], &patterns))
        .await
        .map_err(map_error)?;
    Ok(rows
        .iter()
        .map(|r| SchemaInfo {
            name: r.get(0),
            system: r.get(1),
        })
        .collect())
}

/// The catalog of the server a connection reached, where it differs from PostgreSQL's in ways
/// one query cannot take. CockroachDB's differences are taken by the queries themselves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Catalog {
    PostgreSql,
    /// RisingWave (SpelaionDB's SDB-302): `pg_class` has no `reltuples`, `pg_proc` lists no
    /// user function and has no `pg_get_function_identity_arguments`, oids are `int4`, and a
    /// relation's definition is kept as written in `rw_catalog.rw_relations`.
    RisingWave,
    /// Materialize (SpelaionDB's SDB-303): no `format`, no
    /// `pg_get_function_identity_arguments` and no user functions; sources are listed in
    /// `pg_class` as tables; a relation's definition is what `SHOW CREATE` writes.
    Materialize,
    /// CrateDB (SpelaionDB's SDB-304): oids are `regclass`; no `unnest … with ordinality`, no
    /// foreign keys, no `pg_get_indexdef`, `pg_trigger` or `pg_extension`; user functions are
    /// in `information_schema.routines`; a table's definition is `SHOW CREATE TABLE`'s.
    CrateDb,
}

impl Catalog {
    /// From `select version()`: RisingWave and Materialize name themselves after PostgreSQL's
    /// version (`PostgreSQL 9.5 on … (Materialize 26.50.1)`); CrateDB's starts with its name.
    pub fn from_version(version: &str) -> Self {
        if version.starts_with("CrateDB") {
            Catalog::CrateDb
        } else if version.contains("RisingWave") {
            Catalog::RisingWave
        } else if version.contains("(Materialize ") {
            Catalog::Materialize
        } else {
            Catalog::PostgreSql
        }
    }
}

/// The relation `$1.$2`, by name: no oid is ever read back and sent again, since a server of
/// PostgreSQL's protocol may answer one in another type (RisingWave's are `int4`).
pub(crate) const RELATION: &str = "(select c.oid from pg_class c \
     join pg_namespace n on n.oid = c.relnamespace where n.nspname = $1 and c.relname = $2)";

/// An empty comment is no comment: PostgreSQL never keeps one (`COMMENT … IS ''` removes it),
/// and RisingWave answers `''` for every object.
fn objects_sql(catalog: Catalog) -> String {
    let estimate = match catalog {
        // Materialize and CrateDB keep `reltuples` at -1 for tables: no estimate either.
        Catalog::PostgreSql | Catalog::Materialize | Catalog::CrateDb => {
            "case when c.reltuples < 0 then null else c.reltuples::int8 end"
        }
        Catalog::RisingWave => "null::int8",
    };
    format!(
        "select n.nspname::text, c.relname::text, c.relkind::text, {estimate},
                nullif(obj_description(c.oid, 'pg_class'), '')
         from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
         where c.relkind in ('r', 'p', 'v', 'm', 'f')"
    )
}

fn object_from_row(r: &Row) -> Result<ObjectInfo> {
    let kind: String = r.get(2);
    Ok(ObjectInfo {
        schema: r.get(0),
        name: r.get(1),
        kind: match kind.as_str() {
            "r" => ObjectKind::Table,
            "p" => ObjectKind::PartitionedTable,
            "v" => ObjectKind::View,
            "m" => ObjectKind::MaterializedView,
            "f" => ObjectKind::ForeignTable,
            other => return Err(DriverError::Other(format!("unexpected relkind: {other}"))),
        },
        estimated_rows: r.get(3),
        comment: r.get(4),
    })
}

pub async fn list_objects(
    client: &Client,
    catalog: Catalog,
    schema: &str,
    like: &LikeFilter,
) -> Result<Vec<ObjectInfo>> {
    let mut patterns = Vec::new();
    let narrow = like_sql(like, "c.relname::text", &mut patterns, 1);
    // Partitions are skipped: they will appear under their parent table. `is not true`, not
    // `not`: CockroachDB leaves `relispartition` NULL, and `not NULL` would skip every table.
    let sql = format!(
        "{} and n.nspname = $1 and c.relispartition is not true{narrow} order by c.relname",
        objects_sql(catalog)
    );
    let rows = client
        .query(&sql, &query_params(&[schema], &patterns))
        .await
        .map_err(map_error)?;
    rows.iter().map(object_from_row).collect()
}

/// Every column of the schema's relations (partitions left out, as in `list_objects`).
pub async fn list_columns(client: &Client, schema: &str) -> Result<Vec<SchemaColumn>> {
    let rows = client
        .query(
            "select c.relname::text, a.attname::text, format_type(a.atttypid, a.atttypmod)
             from pg_attribute a
             join pg_class c on c.oid = a.attrelid
             join pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relkind in ('r', 'p', 'v', 'm', 'f')
               and c.relispartition is not true and a.attnum > 0 and not a.attisdropped
             order by c.relname, a.attnum",
            &[&schema],
        )
        .await
        .map_err(map_error)?;
    Ok(rows
        .iter()
        .map(|r| SchemaColumn {
            table: r.get(0),
            name: r.get(1),
            native_type: r.get(2),
        })
        .collect())
}

pub async fn describe_table(
    client: &Client,
    catalog: Catalog,
    schema: &str,
    name: &str,
) -> Result<TableInfo> {
    let sql = format!(
        "{} and n.nspname = $1 and c.relname = $2",
        objects_sql(catalog)
    );
    let row = client
        .query_opt(&sql, &[&schema, &name])
        .await
        .map_err(map_error)?
        .ok_or_else(|| DriverError::Other(format!("{schema}.{name} does not exist")))?;
    let object = object_from_row(&row)?;
    let relation = [&schema as &(dyn ToSql + Sync), &name];

    let columns = client
        .query(
            &format!(
                "select a.attname::text, format_type(a.atttypid, a.atttypmod), not a.attnotnull,
                        pg_get_expr(d.adbin, d.adrelid),
                        nullif(col_description(a.attrelid, a.attnum), '')
                 from pg_attribute a
                 left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
                 where a.attrelid = {RELATION} and a.attnum > 0 and not a.attisdropped
                 order by a.attnum"
            ),
            &relation,
        )
        .await
        .map_err(map_error)?
        .iter()
        .map(|r| ColumnInfo {
            name: r.get(0),
            native_type: r.get(1),
            nullable: r.get(2),
            default: r.get(3),
            comment: r.get(4),
        })
        .collect();

    match catalog {
        // No foreign keys, no index to define, and no `unnest … with ordinality`.
        Catalog::CrateDb => {
            let (primary_key, constraints) = crate_keys(client, schema, name).await?;
            return Ok(TableInfo {
                object,
                columns,
                primary_key,
                indexes: Vec::new(),
                foreign_keys: Vec::new(),
                constraints,
            });
        }
        Catalog::PostgreSql | Catalog::RisingWave | Catalog::Materialize => {}
    }

    // From the primary key's constraint, whose columns are in key order: RisingWave has no
    // `LATERAL`, and lists the table itself in `pg_index` as its primary key's index.
    let primary_key = client
        .query_opt(
            &format!(
                "select array(select a.attname::text
                              from unnest(con.conkey) with ordinality as k(attnum, ord)
                              join pg_attribute a on a.attrelid = con.conrelid and a.attnum = k.attnum
                              order by k.ord)
                 from pg_constraint con
                 where con.conrelid = {RELATION} and con.contype = 'p'"
            ),
            &relation,
        )
        .await
        .map_err(map_error)?
        .map(|r| r.get(0))
        .unwrap_or_default();

    // `indexrelid <> indrelid`: RisingWave's row for a table's own primary key is the table,
    // which `pg_get_indexdef` refuses.
    let indexes = client
        .query(
            &format!(
                "select ic.relname::text, i.indisunique, i.indisprimary, pg_get_indexdef(i.indexrelid)
                 from pg_index i join pg_class ic on ic.oid = i.indexrelid
                 where i.indrelid = {RELATION} and i.indexrelid <> i.indrelid
                 order by ic.relname"
            ),
            &relation,
        )
        .await
        .map_err(map_error)?
        .iter()
        .map(|r| IndexInfo {
            name: r.get(0),
            unique: r.get(1),
            primary: r.get(2),
            definition: r.get(3),
        })
        .collect();

    let foreign_keys = client
        .query(
            &format!(
                "select con.conname::text,
                    array(select a.attname::text from unnest(con.conkey) with ordinality k(n, o)
                          join pg_attribute a on a.attrelid = con.conrelid and a.attnum = k.n order by k.o),
                    rn.nspname::text, rc.relname::text,
                    array(select a.attname::text from unnest(con.confkey) with ordinality k(n, o)
                          join pg_attribute a on a.attrelid = con.confrelid and a.attnum = k.n order by k.o)
             from pg_constraint con
             join pg_class rc on rc.oid = con.confrelid
             join pg_namespace rn on rn.oid = rc.relnamespace
             where con.conrelid = {RELATION} and con.contype = 'f'
             order by con.conname"
            ),
            &relation,
        )
        .await
        .map_err(map_error)?
        .iter()
        .map(|r| ForeignKeyInfo {
            name: r.get(0),
            columns: r.get(1),
            referenced_schema: r.get(2),
            referenced_table: r.get(3),
            referenced_columns: r.get(4),
        })
        .collect();

    // RisingWave writes no definition (`pg_get_constraintdef` is NULL there): a key's is
    // written from its columns, as PostgreSQL would write it.
    let constraints = client
        .query(
            &format!(
                "select c.conname::text, c.contype::text,
                        coalesce(pg_get_constraintdef(c.oid, true),
                                 case c.contype when 'p' then 'PRIMARY KEY' when 'u' then 'UNIQUE' end
                                 || ' (' || array_to_string(array(
                                     select quote_ident(a.attname)
                                     from unnest(c.conkey) with ordinality as k(attnum, ord)
                                     join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.attnum
                                     order by k.ord), ', ') || ')',
                                 ''),
                        array(select a.attname::text
                              from unnest(c.conkey) with ordinality as k(attnum, ord)
                              join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.attnum
                              order by k.ord)
                 from pg_constraint c
                 where c.conrelid = {RELATION} and c.contype in ('p', 'u', 'c')
                 order by c.contype <> 'p', c.conname"
            ),
            &relation,
        )
        .await
        .map_err(map_error)?
        .iter()
        .map(|r| {
            let kind: String = r.get(1);
            let kind = match kind.as_str() {
                "p" => ConstraintKind::PrimaryKey,
                "u" => ConstraintKind::Unique,
                _ => ConstraintKind::Check,
            };
            ConstraintInfo {
                name: Some(r.get(0)),
                kind,
                columns: if kind == ConstraintKind::Check {
                    Vec::new()
                } else {
                    r.get(3)
                },
                definition: r.get(2),
            }
        })
        .collect();

    Ok(TableInfo {
        object,
        columns,
        primary_key,
        indexes,
        foreign_keys,
        constraints,
    })
}

/// CrateDB's primary key and constraints: each constraint from `pg_constraint`, with the
/// definition CrateDB writes, and the columns of a key from `information_schema`, in order
/// (CrateDB has no `unnest … with ordinality`).
async fn crate_keys(
    client: &Client,
    schema: &str,
    name: &str,
) -> Result<(Vec<String>, Vec<ConstraintInfo>)> {
    let read = |e: tokio_postgres::Error| {
        DriverError::Other(format!("reading {schema}.{name}'s constraints: {e}"))
    };
    let mut columns: std::collections::HashMap<String, Vec<String>> = Default::default();
    for row in client
        .query(
            "select constraint_name, column_name from information_schema.key_column_usage
             where table_schema = $1 and table_name = $2
             order by constraint_name, ordinal_position",
            &[&schema, &name],
        )
        .await
        .map_err(map_error)?
    {
        let (constraint, column): (String, String) =
            (row.try_get(0).map_err(read)?, row.try_get(1).map_err(read)?);
        columns.entry(constraint).or_default().push(column);
    }
    let mut primary_key = Vec::new();
    let mut constraints = Vec::new();
    for row in client
        .query(
            &format!(
                "select conname::text, contype::text, pg_get_constraintdef(oid, true)
                 from pg_constraint
                 where conrelid = {RELATION} and contype in ('p', 'u', 'c')
                 order by conname"
            ),
            &[&schema, &name],
        )
        .await
        .map_err(map_error)?
    {
        let conname: String = row.try_get(0).map_err(read)?;
        let contype: String = row.try_get(1).map_err(read)?;
        let definition: Option<String> = row.try_get(2).map_err(read)?;
        let kind = match contype.as_str() {
            "p" => ConstraintKind::PrimaryKey,
            "u" => ConstraintKind::Unique,
            _ => ConstraintKind::Check,
        };
        let key = match kind {
            ConstraintKind::Check => Vec::new(),
            ConstraintKind::PrimaryKey | ConstraintKind::Unique => {
                columns.get(&conname).cloned().unwrap_or_default()
            }
        };
        if kind == ConstraintKind::PrimaryKey {
            primary_key = key.clone();
        }
        constraints.push(ConstraintInfo {
            name: Some(conname),
            kind,
            columns: key,
            definition: definition.unwrap_or_default(),
        });
    }
    // The primary key first, as on PostgreSQL.
    constraints.sort_by_key(|c| c.kind != ConstraintKind::PrimaryKey);
    Ok((primary_key, constraints))
}

/// The foreign keys pointing to `schema.name`. Those of partitions are left out: they are
/// copies of their parent table's key, which is listed. CrateDB has no foreign keys.
pub async fn list_referencing_keys(
    client: &Client,
    catalog: Catalog,
    schema: &str,
    name: &str,
) -> Result<Vec<ReferencingKey>> {
    match catalog {
        Catalog::CrateDb => return Ok(Vec::new()),
        Catalog::PostgreSql | Catalog::RisingWave | Catalog::Materialize => {}
    }
    let rows = client
        .query(
            "select n.nspname::text, c.relname::text, con.conname::text,
                    array(select a.attname::text from unnest(con.conkey) with ordinality k(n, o)
                          join pg_attribute a on a.attrelid = con.conrelid and a.attnum = k.n order by k.o),
                    array(select a.attname::text from unnest(con.confkey) with ordinality k(n, o)
                          join pg_attribute a on a.attrelid = con.confrelid and a.attnum = k.n order by k.o)
             from pg_constraint con
             join pg_class c on c.oid = con.conrelid
             join pg_namespace n on n.oid = c.relnamespace
             where con.contype = 'f' and c.relispartition is not true
               and con.confrelid = (select t.oid from pg_class t
                                    join pg_namespace tn on tn.oid = t.relnamespace
                                    where tn.nspname = $1 and t.relname = $2)
             order by 1, 2, 3",
            &[&schema, &name],
        )
        .await
        .map_err(map_error)?;
    Ok(rows
        .iter()
        .map(|r| ReferencingKey {
            schema: r.get(0),
            table: r.get(1),
            name: r.get(2),
            columns: r.get(3),
            referenced_columns: r.get(4),
        })
        .collect())
}
