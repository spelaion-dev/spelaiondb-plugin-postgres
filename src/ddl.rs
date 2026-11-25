//! `CREATE TABLE` / `VIEW` / `MATERIALIZED VIEW` / `FOREIGN TABLE` for the explorer's
//! "Generate DDL", composed from the catalog: PostgreSQL has no `pg_get_tabledef`. Names are
//! quoted on the server (`quote_ident`, `format('%I')`); expressions come from `pg_get_expr`,
//! `pg_get_constraintdef` and `pg_get_indexdef`, as `pg_dump` writes them.

use spelaion_driver_api::{DriverError, Result, terminated};
use tokio_postgres::Client;

use crate::introspect::{Catalog, RELATION};
use crate::map_error;

pub async fn relation_definition(
    client: &Client,
    catalog: Catalog,
    schema: &str,
    name: &str,
) -> Result<String> {
    match catalog {
        Catalog::PostgreSql => postgres_definition(client, schema, name).await,
        Catalog::RisingWave => risingwave_definition(client, schema, name).await,
        Catalog::Materialize => materialize_definition(client, schema, name).await,
        Catalog::CrateDb => crate_definition(client, schema, name).await,
    }
}

/// CrateDB writes a table's statement itself (`SHOW CREATE TABLE`, with its sharding and
/// settings); a view's query is in `information_schema.views`. `SHOW` takes no parameters, so
/// the name in it is quoted by the server.
async fn crate_definition(client: &Client, schema: &str, name: &str) -> Result<String> {
    let read = |what: &str, e: tokio_postgres::Error| {
        DriverError::Other(format!("reading {schema}.{name}'s {what}: {e}"))
    };
    let row = client
        .query_opt(
            "select c.relkind::text, quote_ident(n.nspname) || '.' || quote_ident(c.relname)
             from pg_class c join pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relname = $2 and c.relkind in ('r', 'v')",
            &[&schema, &name],
        )
        .await
        .map_err(map_error)?
        .ok_or_else(|| DriverError::Other(format!("{schema}.{name} does not exist")))?;
    let kind: String = row.try_get(0).map_err(|e| read("kind", e))?;
    let qualified: String = row.try_get(1).map_err(|e| read("name", e))?;
    if kind == "v" {
        let query: String = client
            .query_one(
                "select view_definition from information_schema.views
                 where table_schema = $1 and table_name = $2",
                &[&schema, &name],
            )
            .await
            .map_err(map_error)?
            .try_get(0)
            .map_err(|e| read("query", e))?;
        let query = query.trim().trim_end_matches(';');
        return Ok(format!("CREATE OR REPLACE VIEW {qualified} AS\n{query};"));
    }
    let sql: String = client
        .query_one(&format!("SHOW CREATE TABLE {qualified}"), &[])
        .await
        .map_err(map_error)?
        .try_get(0)
        .map_err(|e| read("definition", e))?;
    Ok(terminated(&sql))
}

/// Materialize writes each object's statement itself (`SHOW CREATE`), sources included, which
/// `pg_class` lists as tables: which statement to ask for comes from `mz_objects`, then the
/// relation's indexes. `SHOW` takes no parameters, so the name in it is quoted by the server
/// (`quote_ident`) and the kind is one of ours.
async fn materialize_definition(client: &Client, schema: &str, name: &str) -> Result<String> {
    let read = |what: &str, e: tokio_postgres::Error| {
        DriverError::Other(format!("reading {schema}.{name}'s {what}: {e}"))
    };
    let row = client
        .query_opt(
            "select o.id, o.type, quote_ident(s.name) || '.' || quote_ident(o.name)
             from mz_catalog.mz_objects o
             join mz_catalog.mz_schemas s on s.id = o.schema_id
             join mz_catalog.mz_databases d on d.id = s.database_id
             where d.name = current_database() and s.name = $1 and o.name = $2
               and o.type in ('table', 'source', 'view', 'materialized-view')",
            &[&schema, &name],
        )
        .await
        .map_err(map_error)?
        .ok_or_else(|| DriverError::Other(format!("{schema}.{name} does not exist")))?;
    let id: String = row.try_get(0).map_err(|e| read("id", e))?;
    let kind: String = row.try_get(1).map_err(|e| read("kind", e))?;
    let qualified: String = row.try_get(2).map_err(|e| read("name", e))?;
    let statement = match kind.as_str() {
        "table" => "TABLE",
        "source" => "SOURCE",
        "view" => "VIEW",
        "materialized-view" => "MATERIALIZED VIEW",
        other => {
            return Err(DriverError::Other(format!(
                "{schema}.{name} is a {other}, which has no definition here"
            )));
        }
    };
    let mut statements = vec![show_create(client, statement, &qualified).await?];
    let indexes = client
        .query(
            "select quote_ident(s.name) || '.' || quote_ident(i.name)
             from mz_catalog.mz_indexes i
             join mz_catalog.mz_objects io on io.id = i.id
             join mz_catalog.mz_schemas s on s.id = io.schema_id
             where i.on_id = $1
             order by i.name",
            &[&id],
        )
        .await
        .map_err(map_error)?;
    for row in indexes {
        let index: String = row.try_get(0).map_err(|e| read("index", e))?;
        statements.push(show_create(client, "INDEX", &index).await?);
    }
    Ok(statements.join("\n\n"))
}

/// `SHOW CREATE <what> <qualified>`'s statement: its second column.
pub(crate) async fn show_create(client: &Client, what: &str, qualified: &str) -> Result<String> {
    let row = client
        .query_one(&format!("SHOW CREATE {what} {qualified}"), &[])
        .await
        .map_err(map_error)?;
    let sql: String = row
        .try_get(1)
        .map_err(|e| DriverError::Other(format!("reading SHOW CREATE {what}: {e}")))?;
    Ok(terminated(&sql))
}

/// RisingWave keeps each relation's statement as it was written (`rw_catalog.rw_relations`),
/// with what its catalog does not show (sources, connectors, watermarks): that statement, then
/// the table's indexes. RisingWave keeps no comment where `obj_description` reads it.
async fn risingwave_definition(client: &Client, schema: &str, name: &str) -> Result<String> {
    let definition: Option<String> = client
        .query_opt(
            "select r.definition from rw_catalog.rw_relations r
             join pg_namespace n on n.oid = r.schema_id
             where n.nspname = $1 and r.name = $2
               and r.relation_type in ('table', 'view', 'materialized view')",
            &[&schema, &name],
        )
        .await
        .map_err(map_error)?
        .map(|row| row.try_get(0))
        .transpose()
        .map_err(|e| DriverError::Other(format!("reading {schema}.{name}'s definition: {e}")))?;
    let definition =
        definition.ok_or_else(|| DriverError::Other(format!("{schema}.{name} does not exist")))?;
    let mut statements = vec![terminated(&definition)];
    let indexes = client
        .query(
            &format!(
                "select r.definition from rw_catalog.rw_relations r
                 join pg_index i on i.indexrelid = r.id
                 where i.indrelid = {RELATION} and i.indexrelid <> i.indrelid
                 order by r.name"
            ),
            &[&schema, &name],
        )
        .await
        .map_err(map_error)?;
    for row in indexes {
        let index: String = row
            .try_get(0)
            .map_err(|e| DriverError::Other(format!("reading an index's definition: {e}")))?;
        statements.push(terminated(&index));
    }
    Ok(statements.join("\n\n"))
}

async fn postgres_definition(client: &Client, schema: &str, name: &str) -> Result<String> {
    let row = client
        .query_opt(
            "select c.oid, c.relkind::text, format('%I.%I', n.nspname, c.relname),
                    obj_description(c.oid, 'pg_class')
             from pg_class c join pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relname = $2 and c.relkind in ('r', 'p', 'v', 'm', 'f')",
            &[&schema, &name],
        )
        .await
        .map_err(map_error)?
        .ok_or_else(|| DriverError::Other(format!("{schema}.{name} does not exist")))?;
    // An oid in another type is an error, never a panic of the plugin.
    let oid: u32 = row
        .try_get(0)
        .map_err(|e| DriverError::Other(format!("reading {schema}.{name}'s oid: {e}")))?;
    let kind: String = row.get(1);
    let qualified: String = row.get(2);
    let comment: Option<String> = row.get(3);

    let mut statements = Vec::new();
    let what = match kind.as_str() {
        "v" | "m" => {
            let query: String = client
                .query_one("select pg_get_viewdef($1::oid, true)", &[&oid])
                .await
                .map_err(map_error)?
                .get(0);
            let query = query.trim().trim_end_matches(';');
            if kind == "v" {
                statements.push(format!("CREATE OR REPLACE VIEW {qualified} AS\n{query};"));
                "VIEW"
            } else {
                statements.push(format!(
                    "CREATE MATERIALIZED VIEW {qualified} AS\n{query}\nWITH DATA;"
                ));
                "MATERIALIZED VIEW"
            }
        }
        _ => {
            statements.push(create_table(client, oid, &kind, &qualified).await?);
            statements.extend(indexes(client, oid).await?);
            if kind == "f" {
                "FOREIGN TABLE"
            } else {
                "TABLE"
            }
        }
    };
    if let Some(comment) = comment {
        let literal: String = client
            .query_one("select quote_literal($1::text)", &[&comment])
            .await
            .map_err(map_error)?
            .get(0);
        statements.push(format!("COMMENT ON {what} {qualified} IS {literal};"));
    }
    statements.extend(column_comments(client, oid, &qualified).await?);
    Ok(statements.join("\n\n"))
}

async fn create_table(client: &Client, oid: u32, kind: &str, qualified: &str) -> Result<String> {
    let columns = client
        .query(
            "select quote_ident(a.attname), format_type(a.atttypid, a.atttypmod), a.attnotnull,
                    a.attidentity::text, a.attgenerated::text, pg_get_expr(d.adbin, d.adrelid),
                    case when a.attcollation <> 0 and a.attcollation <> t.typcollation
                         then a.attcollation::regcollation::text end
             from pg_attribute a
             join pg_type t on t.oid = a.atttypid
             left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
             where a.attrelid = $1 and a.attnum > 0 and not a.attisdropped
             order by a.attnum",
            &[&oid],
        )
        .await
        .map_err(map_error)?;
    let mut lines: Vec<String> = columns
        .iter()
        .map(|r| {
            let (name, type_): (String, String) = (r.get(0), r.get(1));
            let (not_null, identity, generated): (bool, String, String) =
                (r.get(2), r.get(3), r.get(4));
            let expression: Option<String> = r.get(5);
            let collation: Option<String> = r.get(6);
            let mut line = format!("    {name} {type_}");
            if let Some(collation) = collation {
                line.push_str(&format!(" COLLATE {collation}"));
            }
            match (identity.as_str(), generated.as_str(), expression) {
                ("a", _, _) => line.push_str(" GENERATED ALWAYS AS IDENTITY"),
                ("d", _, _) => line.push_str(" GENERATED BY DEFAULT AS IDENTITY"),
                (_, "s", Some(expression)) => {
                    line.push_str(&format!(" GENERATED ALWAYS AS ({expression}) STORED"));
                }
                (_, _, Some(expression)) => line.push_str(&format!(" DEFAULT {expression}")),
                (_, _, None) => {}
            }
            if not_null {
                line.push_str(" NOT NULL");
            }
            line
        })
        .collect();
    // Primary key, unique, check, foreign keys and exclusions, as pg_dump orders them.
    let constraints = client
        .query(
            "select quote_ident(conname), pg_get_constraintdef(oid, true)
             from pg_constraint
             where conrelid = $1 and contype in ('p', 'u', 'c', 'f', 'x')
             order by case contype when 'p' then 0 when 'u' then 1 when 'c' then 2
                                   when 'x' then 3 else 4 end, conname",
            &[&oid],
        )
        .await
        .map_err(map_error)?;
    lines.extend(constraints.iter().map(|r| {
        let (name, definition): (String, String) = (r.get(0), r.get(1));
        format!("    CONSTRAINT {name} {definition}")
    }));
    let mut sql = format!(
        "CREATE {}TABLE {qualified} (\n{}\n)",
        if kind == "f" { "FOREIGN " } else { "" },
        lines.join(",\n")
    );
    if kind == "p" {
        let key: String = client
            .query_one("select pg_get_partkeydef($1::oid)", &[&oid])
            .await
            .map_err(map_error)?
            .get(0);
        sql.push_str(&format!("\nPARTITION BY {key}"));
    }
    if kind == "f" {
        let row = client
            .query_one(
                "select quote_ident(s.srvname),
                        (select string_agg(quote_ident(split_part(o, '=', 1)) || ' '
                                           || quote_literal(substr(o, strpos(o, '=') + 1)), ', ')
                         from unnest(ft.ftoptions) as o)
                 from pg_foreign_table ft join pg_foreign_server s on s.oid = ft.ftserver
                 where ft.ftrelid = $1",
                &[&oid],
            )
            .await
            .map_err(map_error)?;
        let (server, options): (String, Option<String>) = (row.get(0), row.get(1));
        sql.push_str(&format!("\nSERVER {server}"));
        if let Some(options) = options {
            sql.push_str(&format!("\nOPTIONS ({options})"));
        }
    }
    Ok(terminated(&sql))
}

/// The indexes that are not behind a constraint (those are in `CREATE TABLE`).
async fn indexes(client: &Client, oid: u32) -> Result<Vec<String>> {
    let rows = client
        .query(
            "select pg_get_indexdef(i.indexrelid)
             from pg_index i join pg_class c on c.oid = i.indexrelid
             where i.indrelid = $1
               and not exists (select 1 from pg_constraint k
                               where k.conrelid = i.indrelid and k.conindid = i.indexrelid
                                 and k.contype in ('p', 'u', 'x'))
             order by c.relname",
            &[&oid],
        )
        .await
        .map_err(map_error)?;
    Ok(rows
        .iter()
        .map(|r| terminated(&r.get::<_, String>(0)))
        .collect())
}

async fn column_comments(client: &Client, oid: u32, qualified: &str) -> Result<Vec<String>> {
    let rows = client
        .query(
            "select quote_ident(a.attname), quote_literal(col_description(a.attrelid, a.attnum))
             from pg_attribute a
             where a.attrelid = $1 and a.attnum > 0 and not a.attisdropped
               and col_description(a.attrelid, a.attnum) is not null
             order by a.attnum",
            &[&oid],
        )
        .await
        .map_err(map_error)?;
    Ok(rows
        .iter()
        .map(|r| {
            let (column, literal): (String, String) = (r.get(0), r.get(1));
            format!("COMMENT ON COLUMN {qualified}.{column} IS {literal};")
        })
        .collect())
}
