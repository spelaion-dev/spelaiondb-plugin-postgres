# tokio-postgres 0.7.18, patched for SpelaionDB

This is the crates.io release (MIT or Apache-2.0), used through `[patch.crates-io]`.
The client exposes `transaction_status()`, the last PostgreSQL ReadyForQuery status,
returning `None` while requests are outstanding. The connection records the status as it
drains each response, including responses whose receiver was dropped. This lets the driver
avoid issuing a keep-alive within a transaction, including an aborted one. The status is
shared using atomics; no credentials or query content are stored.

Second patch (SpelaionDB's SDB-396): `Client::set_unnamed_statements`, for a server reached
through a pooler in transaction mode (PgBouncer's `pool_mode = transaction`), where two
transactions of one client may run on two server connections. When on, `prepare` parses the
unnamed statement (Parse, Describe, Sync) only to learn its parameter and column types, and
keeps that Parse message (`Statement::parse`). Every Bind of the statement (queries, portals,
`COPY` in and out) then sends the Parse again in the same message batch, before its Bind and the
batch's Sync, and reads its ParseComplete before BindComplete (`query::bind_complete`). Each use
is complete on whichever server connection runs it, no named statement is left on a server
connection, and no Close is sent. Off, nothing changes. The driver turns it on when the core
sends the `pooler = transaction` option; `tests/pgbouncer.rs` checks both ways against a
PgBouncer that keeps no prepared statements.

When upgrading, reapply the patch or replace it with an upstream API that reports the same
protocol status. Run the PostgreSQL driver and core health tests against a real server,
including the checks for an idle transaction and a failed transaction.
