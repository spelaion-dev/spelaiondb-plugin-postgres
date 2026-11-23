//! Materialize speaks PostgreSQL's protocol and is reached through this driver (SpelaionDB's
//! SDB-303). Its catalog has no `format`, no `pg_get_function_identity_arguments` and no user
//! functions; it lists sources in `pg_class` as tables, and writes each object's definition
//! itself (`SHOW CREATE`).
//!
//! They need `SPELAION_TEST_MATERIALIZE=host:port` (`materialize/materialized:v26.50.1`, as
//! SpelaionDB's `testing/docker/pg-family` starts it: user and database `materialize`, no
//! password). CI does not start it; without the variable they are skipped with a notice.

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, ObjectKind,
    ServerObjectKind,
};
use spelaion_driver_postgres::PostgresDriver;

async fn connect() -> Option<Box<dyn Connection>> {
    let Ok(addr) = std::env::var("SPELAION_TEST_MATERIALIZE") else {
        eprintln!("SPELAION_TEST_MATERIALIZE not set: test skipped");
        return None;
    };
    let (host, port) = addr
        .rsplit_once(':')
        .expect("SPELAION_TEST_MATERIALIZE must be host:port");
    let params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("port"),
        user: "materialize".into(),
        database: "materialize".into(),
        ..Default::default()
    };
    Some(
        PostgresDriver
            .connect(&params)
            .await
            .expect("connecting to the test Materialize"),
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

#[tokio::test]
async fn the_explorer_reads_materializes_catalog() {
    let Some(conn) = connect().await else {
        return;
    };
    let conn = conn.as_ref();
    let id = std::process::id();
    let (table, view, mv, index, source) = (
        format!("mz_table_{id}"),
        format!("mz_view_{id}"),
        format!("mz_mv_{id}"),
        format!("mz_index_{id}"),
        format!("mz_hook_{id}"),
    );
    for sql in [
        format!("create table {table} (id int not null, name text)"),
        format!("create index {index} on {table} (name)"),
        format!("create view {view} as select id, name from {table}"),
        format!("create materialized view {mv} as select count(*) as n from {table}"),
        format!("create source {source} from webhook body format text"),
        format!("comment on table {table} is 'a table'"),
    ] {
        run(conn, &sql).await.unwrap();
    }
    let checks = async {
        let objects = conn.list_objects("public").await?;
        let object = |name: &str| objects.iter().find(|o| o.name == name);
        assert_eq!(object(&table).map(|o| o.kind), Some(ObjectKind::Table));
        assert_eq!(
            object(&table).and_then(|o| o.comment.as_deref()),
            Some("a table")
        );
        assert_eq!(object(&view).map(|o| o.kind), Some(ObjectKind::View));
        assert_eq!(
            object(&mv).map(|o| o.kind),
            Some(ObjectKind::MaterializedView)
        );
        // A source is a relation that can be read: `pg_class` lists it as a table.
        assert_eq!(object(&source).map(|o| o.kind), Some(ObjectKind::Table));

        let info = conn.describe_table("public", &table).await?;
        let columns: Vec<_> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(columns, ["id", "name"]);
        assert!(!info.columns[0].nullable);
        assert!(info.primary_key.is_empty());
        assert_eq!(info.indexes.len(), 1, "{:?}", info.indexes);
        assert_eq!(info.indexes[0].name, index);
        let all = conn.list_columns("public").await?;
        assert!(all.iter().any(|c| c.table == table && c.name == "name"));
        assert!(
            conn.list_referencing_keys("public", &table)
                .await?
                .is_empty()
        );

        // No user functions, sequences, triggers or types: an empty folder, not an error.
        let schema_objects = conn.list_schema_objects("public").await?;
        assert!(schema_objects.is_empty(), "{schema_objects:?}");
        let server = conn.list_server_objects().await?;
        assert!(
            server
                .iter()
                .any(|o| o.kind == ServerObjectKind::Role && o.name == "materialize")
        );

        // What `SHOW CREATE` writes (it breaks long statements over lines), then the
        // table's index.
        let ddl = conn.relation_definition("public", &table).await?;
        assert!(
            ddl.starts_with("CREATE TABLE") && ddl.contains(&format!("materialize.public.{table}")),
            "{ddl}"
        );
        assert!(ddl.contains(&format!("CREATE INDEX {index} ")), "{ddl}");
        let ddl = conn.relation_definition("public", &view).await?;
        assert!(
            ddl.starts_with("CREATE VIEW") && ddl.contains(&format!("materialize.public.{view}")),
            "{ddl}"
        );
        let ddl = conn.relation_definition("public", &mv).await?;
        assert!(
            ddl.starts_with("CREATE MATERIALIZED VIEW")
                && ddl.contains(&format!("materialize.public.{mv}")),
            "{ddl}"
        );
        let ddl = conn.relation_definition("public", &source).await?;
        assert!(
            ddl.starts_with("CREATE SOURCE")
                && ddl.contains(&format!("materialize.public.{source}")),
            "{ddl}"
        );
        let missing = conn.relation_definition("public", "mz_no_such_table").await;
        assert!(
            matches!(&missing, Err(DriverError::Other(m)) if m.contains("does not exist")),
            "{missing:?}"
        );
        Ok::<_, DriverError>(())
    };
    let result = checks.await;
    for sql in [
        format!("drop source if exists {source}"),
        format!("drop materialized view if exists {mv}"),
        format!("drop view if exists {view}"),
        format!("drop table if exists {table} cascade"),
    ] {
        let _ = run(conn, &sql).await;
    }
    result.unwrap();
}
