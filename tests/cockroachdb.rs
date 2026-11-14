//! CockroachDB speaks PostgreSQL's protocol and is reached through this driver (SpelaionDB's
//! SDB-169). Its catalog is PostgreSQL's with differences the explorer's queries must take:
//! `relispartition` is NULL, and there is no `pg_tablespace_location`.
//!
//! They need `SPELAION_TEST_COCKROACH=host:port` (`testing/compose.yaml`, user `root`, no
//! password). Without the variable they are skipped with a notice.

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, ObjectKind,
    SchemaObjectKind, ServerObjectKind,
};
use spelaion_driver_postgres::PostgresDriver;

async fn connect() -> Option<Box<dyn Connection>> {
    let Ok(addr) = std::env::var("SPELAION_TEST_COCKROACH") else {
        eprintln!("SPELAION_TEST_COCKROACH not set: test skipped");
        return None;
    };
    let (host, port) = addr
        .rsplit_once(':')
        .expect("SPELAION_TEST_COCKROACH must be host:port");
    let params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("port"),
        user: "root".into(),
        database: "defaultdb".into(),
        ..Default::default()
    };
    Some(
        PostgresDriver
            .connect(&params)
            .await
            .expect("connecting to the test CockroachDB"),
    )
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

#[tokio::test]
async fn the_explorer_reads_cockroachdbs_catalog() {
    let Some(conn) = connect().await else {
        return;
    };
    let conn = conn.as_ref();
    let id = std::process::id();
    let (parent, child, view, seq) = (
        format!("crdb_parent_{id}"),
        format!("crdb_child_{id}"),
        format!("crdb_view_{id}"),
        format!("crdb_seq_{id}"),
    );
    for sql in [
        format!("create table {parent} (id int primary key, name text)"),
        format!("create table {child} (id int primary key, parent int references {parent} (id))"),
        format!("create view {view} as select id, name from {parent}"),
        format!("create sequence {seq}"),
    ] {
        run(conn, &sql).await.unwrap();
    }
    let checks = async {
        // `relispartition` is NULL in CockroachDB: no table may be taken for a partition.
        let objects = conn.list_objects("public").await?;
        let kind = |name: &str| objects.iter().find(|o| o.name == name).map(|o| o.kind);
        assert_eq!(kind(&parent), Some(ObjectKind::Table));
        assert_eq!(kind(&child), Some(ObjectKind::Table));
        assert_eq!(kind(&view), Some(ObjectKind::View));
        let columns = conn.list_columns("public").await?;
        assert!(
            columns
                .iter()
                .any(|c| c.table == child && c.name == "parent")
        );
        let keys = conn.list_referencing_keys("public", &parent).await?;
        assert!(
            keys.iter().any(|k| k.table == child
                && k.columns == ["parent"]
                && k.referenced_columns == ["id"]),
            "{keys:?}"
        );
        let schema_objects = conn.list_schema_objects("public").await?;
        assert!(
            schema_objects
                .iter()
                .any(|o| o.name == seq && o.kind == SchemaObjectKind::Sequence)
        );
        // No `pg_tablespace_location`: tablespaces are listed without their folder.
        let server = conn.list_server_objects().await?;
        let tablespace = server
            .iter()
            .find(|o| o.kind == ServerObjectKind::Tablespace)
            .expect("a tablespace");
        assert_eq!(tablespace.detail, None);
        assert!(
            server
                .iter()
                .any(|o| o.kind == ServerObjectKind::Role && o.name == "root")
        );
        Ok::<_, DriverError>(())
    };
    let result = checks.await;
    for sql in [
        format!("drop view {view}"),
        format!("drop table {child}"),
        format!("drop table {parent}"),
        format!("drop sequence {seq}"),
    ] {
        let _ = run(conn, &sql).await;
    }
    result.unwrap();
}
