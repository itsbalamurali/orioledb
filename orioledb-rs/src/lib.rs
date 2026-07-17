use pgrx::prelude::*;

::pgrx::pg_module_magic!(name, version);

pub mod btree;
pub mod catalog;
pub mod checkpoint;
pub mod indexam;
pub mod recovery;
pub mod rewind;
pub mod s3;
pub mod tableam;
pub mod transam;
pub mod tuple;
pub mod utils;
pub mod workers;

#[pg_extern]
fn hello_orioledb() -> &'static str {
    "Hello, orioledb"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_hello_orioledb() {
        assert_eq!("Hello, orioledb", crate::hello_orioledb());
    }

}

#[cfg(feature = "pg_bench")]
#[pg_schema]
mod benches {
    use pgrx::prelude::*;
    use pgrx_bench::{Bencher, black_box};

    #[pg_bench]
    fn bench_hello_orioledb(b: &mut Bencher) {
        b.iter(|| {
            black_box(crate::hello_orioledb());
        });
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
