//! The server's databases (SpelaionDB's ADR-074, SDB-489): `list_databases` answers the
//! databases this user may open, never a template, never one that refuses connections, and not
//! one whose `CONNECT` was taken from the user; a session still never changes database.
//!
//! Needs `SPELAION_TEST_PG=host:port` (user, password and database: `spelaion`, a superuser);
//! without it the test is skipped, which does not count as tested.

use futures_util::TryStreamExt;
use spelaion_driver_api::{ConnectParams, Connection, Driver, ExecEvent, ExecOptions, Secret};
use spelaion_driver_postgres::PostgresDriver;

/// Names of this test's own objects, apart from SpelaionDB's repository's test of the same.
const NO_CONNECT: &str = "plugin_dblist_no_connect";
const CLOSED: &str = "plugin_dblist_closed";
const ROLE: &str = "plugin_dblist_role";

fn server() -> Option<(String, u16)> {
    let Ok(addr) = std::env::var("SPELAION_TEST_PG") else {
        eprintln!("SPELAION_TEST_PG not set: test skipped");
        return None;
    };
    let (host, port) = addr
        .rsplit_once(':')
        .expect("SPELAION_TEST_PG must be host:port");
    Some((host.to_string(), port.parse().expect("port")))
}

async fn connect(host: &str, port: u16, user: &str, password: &str) -> Box<dyn Connection> {
    PostgresDriver
        .connect(&ConnectParams {
            host: host.into(),
            port,
            user: user.into(),
            password: Some(Secret::new(password)),
            database: "spelaion".into(),
            ..Default::default()
        })
        .await
        .unwrap_or_else(|e| panic!("connecting as {user}: {e}"))
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<(), String> {
    let _: Vec<ExecEvent> = conn
        .execute_bound(sql, Vec::new(), &ExecOptions::default())
        .await
        .map_err(|e| e.to_string())?
        .try_collect()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

async fn clean(admin: &dyn Connection) {
    for sql in [
        format!("DROP DATABASE IF EXISTS {NO_CONNECT}"),
        format!("DROP DATABASE IF EXISTS {CLOSED}"),
        format!("DROP ROLE IF EXISTS {ROLE}"),
    ] {
        run(admin, &sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

#[tokio::test]
async fn lists_the_databases_the_user_may_open() {
    let Some((host, port)) = server() else {
        return;
    };
    let admin = connect(&host, port, "spelaion", "spelaion").await;
    clean(admin.as_ref()).await;
    for sql in [
        format!("CREATE DATABASE {NO_CONNECT}"),
        format!("REVOKE CONNECT ON DATABASE {NO_CONNECT} FROM PUBLIC"),
        format!("CREATE DATABASE {CLOSED}"),
        format!("ALTER DATABASE {CLOSED} ALLOW_CONNECTIONS false"),
        format!("CREATE ROLE {ROLE} LOGIN PASSWORD 'dblist'"),
    ] {
        run(admin.as_ref(), &sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    let user = connect(&host, port, ROLE, "dblist").await;
    let listed = user.list_databases().await;
    let everything = admin.list_databases().await;
    drop(user);
    clean(admin.as_ref()).await;
    let listed = listed.expect("the user's databases");
    let everything = everything.expect("the superuser's databases");

    for name in ["spelaion", "spelaion_gis", "postgres"] {
        assert!(listed.iter().any(|d| d == name), "{name} in {listed:?}");
    }
    for name in ["template0", "template1", NO_CONNECT, CLOSED] {
        assert!(
            !listed.iter().any(|d| d == name),
            "{name} not in {listed:?}"
        );
    }
    // A superuser has CONNECT everywhere; templates and a closed database are still left out.
    assert!(everything.iter().any(|d| d == NO_CONNECT), "{everything:?}");
    for name in ["template0", "template1", CLOSED] {
        assert!(
            !everything.iter().any(|d| d == name),
            "{name} not in {everything:?}"
        );
    }
    let mut sorted = listed.clone();
    sorted.sort();
    assert_eq!(listed, sorted, "ordered by name");

    // Another database is another session: this one never changes database.
    assert!(admin.select_database("postgres").await.is_err());
}
