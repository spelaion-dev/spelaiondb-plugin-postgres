//! PostgreSQL through PgBouncer in transaction mode (SpelaionDB's SDB-396), against
//! `testing/compose.yaml`'s `pgbouncer` (`SPELAION_TEST_PGBOUNCER=127.0.0.1:16432`), which keeps
//! no prepared statement for a client and hands each transaction to the server connection used
//! least recently. With the core's option `pooler = transaction` the driver relies on no named
//! prepared statement (the patched client sends each statement's Parse with its Bind), so
//! statements, bound parameters, catalog reads, a transaction and `COPY` all work; without it,
//! a statement is lost, which shows the setup is hard enough. Skipped without the variable.

use std::collections::BTreeMap;

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, ExecEvent, ExecOptions, InsertTarget, Secret, TextRows,
    TlsMode,
};
use spelaion_driver_postgres::PostgresDriver;

async fn connect(pooler: Option<&str>) -> Option<Box<dyn Connection>> {
    let Ok(addr) = std::env::var("SPELAION_TEST_PGBOUNCER") else {
        eprintln!("SPELAION_TEST_PGBOUNCER not set: test skipped");
        return None;
    };
    let (host, port) = addr
        .rsplit_once(':')
        .expect("SPELAION_TEST_PGBOUNCER must be host:port");
    let mut options = BTreeMap::new();
    if let Some(mode) = pooler {
        options.insert("pooler".to_string(), mode.to_string());
    }
    let params = ConnectParams {
        host: host.into(),
        port: port.parse().expect("port"),
        user: "spelaion".into(),
        password: Some(Secret::new("spelaion")),
        database: "spelaion".into(),
        tls: TlsMode::Disable,
        options,
        ..Default::default()
    };
    Some(
        PostgresDriver
            .connect(&params)
            .await
            .expect("connecting through the test PgBouncer"),
    )
}

/// The first cell of `sql`'s first row, as text.
async fn first(conn: &dyn Connection, sql: &str, params: &[&str]) -> Result<String, String> {
    let events: Vec<ExecEvent> = conn
        .execute_bound(
            sql,
            params.iter().map(|p| p.to_string()).collect(),
            &ExecOptions::default(),
        )
        .await
        .map_err(|e| e.to_string())?
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;
    events
        .iter()
        .find_map(|event| match event {
            ExecEvent::Batch(batch) if batch.num_rows() > 0 => {
                Some(arrow::util::display::array_value_to_string(batch.column(0), 0).unwrap())
            }
            _ => None,
        })
        .ok_or_else(|| "no row".into())
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<(), String> {
    conn.execute(sql, &ExecOptions::default())
        .await
        .map_err(|e| e.to_string())?
        .try_collect::<Vec<_>>()
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn behind_a_transaction_pooler_nothing_relies_on_a_named_statement() {
    let Some(conn) = connect(Some("transaction")).await else {
        return;
    };
    let conn = conn.as_ref();
    for n in 0..40 {
        assert_eq!(
            first(conn, "select $1::int * 2", &[&n.to_string()])
                .await
                .unwrap(),
            (n * 2).to_string()
        );
        assert_eq!(
            first(conn, &format!("select {n}"), &[]).await.unwrap(),
            n.to_string()
        );
    }
    for _ in 0..5 {
        conn.list_schemas().await.unwrap();
        conn.list_objects("public").await.unwrap();
        conn.list_columns("public").await.unwrap();
    }

    let table = format!("pooled_{}", std::process::id());
    run(
        conn,
        &format!("create table {table} (id int primary key, name text)"),
    )
    .await
    .unwrap();
    // A transaction keeps one server connection from BEGIN to COMMIT.
    run(conn, "begin").await.unwrap();
    run(conn, &format!("insert into {table} values (0, 'zero')"))
        .await
        .unwrap();
    assert_eq!(
        first(conn, &format!("select count(*) from {table}"), &[])
            .await
            .unwrap(),
        "1"
    );
    run(conn, "commit").await.unwrap();
    // COPY … FROM STDIN outside a transaction: its statement is parsed with it.
    let rows: Vec<Vec<Option<String>>> = (1..=500)
        .map(|n| vec![Some(n.to_string()), Some(format!("row {n}"))])
        .collect();
    let loaded = conn
        .insert_rows(
            &InsertTarget {
                schema: "public".into(),
                table: table.clone(),
                columns: vec!["id".into(), "name".into()],
            },
            TextRows::from_rows(2, &rows).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(loaded, 500);
    assert_eq!(
        first(conn, &format!("select count(*) from {table}"), &[])
            .await
            .unwrap(),
        "501"
    );
    run(conn, &format!("drop table {table}")).await.unwrap();
}

#[tokio::test]
async fn without_the_option_a_transaction_pooler_loses_a_named_statement() {
    let Some(conn) = connect(None).await else {
        return;
    };
    let mut failure = None;
    for n in 0..40 {
        if let Err(error) = first(conn.as_ref(), "select $1::int", &[&n.to_string()]).await {
            failure = Some(error);
            break;
        }
    }
    let failure = failure.expect("a named statement was lost at least once");
    assert!(failure.contains("prepared statement"), "{failure}");
}
