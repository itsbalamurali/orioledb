//! OrioleDB recovery subsystem — WAL reading, replay, logical replication, and worker management.

pub mod logical;
pub mod wal;
pub mod wal_reader;
pub mod worker;
