//! CrateDB speaks PostgreSQL's protocol and is reached through this driver (SpelaionDB's
//! SDB-304). It has no transactions (`BEGIN` is accepted and ignored, `ROLLBACK` is a syntax
//! error: SpelaionDB's core follows no transaction there), its oids are `regclass`, and its
//! catalog lacks much of PostgreSQL's.
//!
//! They need `SPELAION_TEST_CRATEDB=host:port` (`crate:6.5.1`, as SpelaionDB's
//! `testing/docker/pg-family` starts it: user `crate`, schema `doc`, no password). CI does not
//! start it; without the variable they are skipped with a notice.

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, ConstraintKind, Driver, DriverError, ExecEvent, ExecOptions,
    ObjectKind, SchemaObjectKind, ServerObjectKind,
};
use spelaion_driver_postgres::PostgresDriver;

async fn connect() -> Option<Box<dyn Connection>> {
    let Ok(addr) = std::env::var("SPELAION_TEST_CRATEDB") else {
        eprintln!("SPELAION_TEST_CRATEDB not set: test skipped");
        return None;
    };
    let (host, port) = addr
        .rsplit_once(':')
        .expect("SPELAION_TEST_CRATEDB must be host:port");
    let params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("port"),
        user: "crate".into(),
        database: "doc".into(),
        ..Default::default()
    };
    Some(
        PostgresDriver
            .connect(&params)
            .await
            .expect("connecting to the test CrateDB"),
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

#[tokio::test]
async fn the_explorer_reads_cratedbs_catalog() {
    let Some(conn) = connect().await else {
        return;
    };
    let conn = conn.as_ref();
    let id = std::process::id();
    let (table, view, function) = (
        format!("crate_table_{id}"),
        format!("crate_view_{id}"),
        format!("crate_fn_{id}"),
    );
    for sql in [
        format!("create table doc.{table} (id int primary key, name text)"),
        format!("create view doc.{view} as select id, name from doc.{table}"),
        format!(
            "create function doc.{function}(a integer, b integer) returns integer \
             language javascript as 'function {function}(a, b) {{ return a + b; }}'"
        ),
    ] {
        run(conn, &sql).await.unwrap();
    }
    let checks = async {
        let schemas = conn.list_schemas().await?;
        assert!(schemas.iter().any(|s| s.name == "doc"), "{schemas:?}");
        let objects = conn.list_objects("doc").await?;
        let object = |name: &str| objects.iter().find(|o| o.name == name);
        assert_eq!(object(&table).map(|o| o.kind), Some(ObjectKind::Table));
        assert_eq!(object(&view).map(|o| o.kind), Some(ObjectKind::View));

        let info = conn.describe_table("doc", &table).await?;
        let columns: Vec<_> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(columns, ["id", "name"]);
        assert_eq!(info.primary_key, ["id"]);
        let key = info
            .constraints
            .iter()
            .find(|c| c.kind == ConstraintKind::PrimaryKey)
            .expect("the primary key");
        assert_eq!(key.columns, ["id"]);
        let all = conn.list_columns("doc").await?;
        assert!(all.iter().any(|c| c.table == table && c.name == "name"));
        assert!(conn.list_referencing_keys("doc", &table).await?.is_empty());
        // Its user functions, from `information_schema.routines`, with their arguments.
        let schema_objects = conn.list_schema_objects("doc").await?;
        let udf = schema_objects
            .iter()
            .find(|o| o.name == function)
            .unwrap_or_else(|| panic!("{function} in {schema_objects:?}"));
        assert_eq!(udf.kind, SchemaObjectKind::Function);
        assert_eq!(udf.detail.as_deref(), Some("integer, integer"));
        let definition = conn.object_definition(udf).await?;
        assert!(
            definition.starts_with(&format!(
                "CREATE OR REPLACE FUNCTION doc.{function}(integer, integer)"
            )),
            "{definition}"
        );
        assert!(definition.contains("LANGUAGE JAVASCRIPT"), "{definition}");
        assert!(definition.contains("return a + b;"), "{definition}");
        let server = conn.list_server_objects().await?;
        assert!(
            server
                .iter()
                .any(|o| o.kind == ServerObjectKind::Role && o.name == "crate"),
            "{server:?}"
        );
        let ddl = conn.relation_definition("doc", &table).await?;
        assert!(ddl.starts_with("CREATE TABLE"), "{ddl}");
        assert!(ddl.contains(&table), "{ddl}");
        let ddl = conn.relation_definition("doc", &view).await?;
        assert!(ddl.starts_with("CREATE"), "{ddl}");
        assert!(ddl.contains(&view), "{ddl}");
        Ok::<_, DriverError>(())
    };
    let result = checks.await;
    for sql in [
        format!("drop function if exists doc.{function}(integer, integer)"),
        format!("drop view if exists doc.{view}"),
        format!("drop table if exists doc.{table}"),
    ] {
        let _ = run(conn, &sql).await;
    }
    result.unwrap();
}
