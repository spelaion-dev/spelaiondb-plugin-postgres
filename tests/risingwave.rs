//! RisingWave speaks PostgreSQL's protocol and is reached through this driver (SpelaionDB's
//! SDB-302). Its catalog is PostgreSQL's with more missing than CockroachDB's: no `reltuples`,
//! no `LATERAL`, oids that arrive as `int4`, no user function in `pg_proc`, no constraint
//! definitions and no comments where `obj_description` reads them. A relation's definition is
//! the statement RisingWave kept.
//!
//! They need `SPELAION_TEST_RISINGWAVE=host:port` (`risingwavelabs/risingwave:v3.1.1
//! single_node`, as SpelaionDB's `testing/docker/pg-family` starts it: user `root`, database
//! `dev`, no password). Its image is about 11 GB, so CI does not start it; without the
//! variable they are skipped with a notice.

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, ConstraintKind, Driver, DriverError, ExecEvent, ExecOptions,
    ObjectKind, ServerObjectKind,
};
use spelaion_driver_postgres::PostgresDriver;

async fn connect() -> Option<Box<dyn Connection>> {
    let Ok(addr) = std::env::var("SPELAION_TEST_RISINGWAVE") else {
        eprintln!("SPELAION_TEST_RISINGWAVE not set: test skipped");
        return None;
    };
    let (host, port) = addr
        .rsplit_once(':')
        .expect("SPELAION_TEST_RISINGWAVE must be host:port");
    let params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("port"),
        user: "root".into(),
        database: "dev".into(),
        ..Default::default()
    };
    Some(
        PostgresDriver
            .connect(&params)
            .await
            .expect("connecting to the test RisingWave"),
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

#[tokio::test]
async fn the_explorer_reads_risingwaves_catalog() {
    let Some(conn) = connect().await else {
        return;
    };
    let conn = conn.as_ref();
    let id = std::process::id();
    let (table, view, mv, index) = (
        format!("rw_table_{id}"),
        format!("rw_view_{id}"),
        format!("rw_mv_{id}"),
        format!("rw_index_{id}"),
    );
    for sql in [
        format!("create table {table} (id int primary key, name varchar)"),
        format!("create index {index} on {table} (name)"),
        format!("create view {view} as select id, name from {table}"),
        format!("create materialized view {mv} as select count(*) as n from {table}"),
    ] {
        run(conn, &sql).await.unwrap();
    }
    let checks = async {
        // No `reltuples`: tables, views and materialized views come without an estimate.
        let objects = conn.list_objects("public").await?;
        let object = |name: &str| objects.iter().find(|o| o.name == name);
        assert_eq!(object(&table).map(|o| o.kind), Some(ObjectKind::Table));
        assert_eq!(object(&view).map(|o| o.kind), Some(ObjectKind::View));
        assert_eq!(
            object(&mv).map(|o| o.kind),
            Some(ObjectKind::MaterializedView)
        );
        assert!(objects.iter().all(|o| o.estimated_rows.is_none()));
        assert!(objects.iter().all(|o| o.comment.is_none()), "{objects:?}");

        // Oids arrive as `int4`, and the table is its own primary key's row in `pg_index`.
        let info = conn.describe_table("public", &table).await?;
        let columns: Vec<_> = info.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(columns, ["id", "name"]);
        assert!(info.columns.iter().all(|c| c.comment.is_none()));
        assert_eq!(info.primary_key, ["id"]);
        let key = info
            .constraints
            .iter()
            .find(|c| c.kind == ConstraintKind::PrimaryKey)
            .expect("the primary key");
        assert_eq!(key.columns, ["id"]);
        assert_eq!(key.definition, "PRIMARY KEY (id)");
        assert_eq!(info.indexes.len(), 1, "{:?}", info.indexes);
        assert_eq!(info.indexes[0].name, index);
        assert!(info.foreign_keys.is_empty());
        let all = conn.list_columns("public").await?;
        assert!(all.iter().any(|c| c.table == table && c.name == "name"));
        assert!(
            conn.list_referencing_keys("public", &table)
                .await?
                .is_empty()
        );

        // No user function in `pg_proc`, no sequences, triggers or types: an empty folder.
        let schema_objects = conn.list_schema_objects("public").await?;
        assert!(schema_objects.is_empty(), "{schema_objects:?}");
        let server = conn.list_server_objects().await?;
        assert!(
            server
                .iter()
                .any(|o| o.kind == ServerObjectKind::Role && o.name == "root")
        );

        // The statements RisingWave kept, then the table's index.
        let ddl = conn.relation_definition("public", &table).await?;
        assert!(
            ddl.starts_with(&format!("CREATE TABLE {table} (id INT PRIMARY KEY")),
            "{ddl}"
        );
        assert!(
            ddl.contains(&format!("CREATE INDEX {index} ON {table}(name);")),
            "{ddl}"
        );
        let ddl = conn.relation_definition("public", &view).await?;
        assert_eq!(
            ddl,
            format!("CREATE VIEW {view} AS SELECT id, name FROM {table};")
        );
        let ddl = conn.relation_definition("public", &mv).await?;
        assert!(
            ddl.starts_with(&format!("CREATE MATERIALIZED VIEW {mv} AS")),
            "{ddl}"
        );
        let missing = conn.relation_definition("public", "rw_no_such_table").await;
        assert!(
            matches!(&missing, Err(DriverError::Other(m)) if m.contains("does not exist")),
            "{missing:?}"
        );
        Ok::<_, DriverError>(())
    };
    let result = checks.await;
    for sql in [
        format!("drop materialized view {mv}"),
        format!("drop view {view}"),
        format!("drop index {index}"),
        format!("drop table {table}"),
    ] {
        let _ = run(conn, &sql).await;
    }
    result.unwrap();
}
