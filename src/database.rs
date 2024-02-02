use postgres::{Client, NoTls};
use r2d2::{Pool, PooledConnection};
use r2d2_postgres::PostgresConnectionManager;
use polars::prelude::*;

// Good reference https://github.com/sfu-db/connector-x/blob/05283cfe6a699d50dfefe345fb9d06c9667dc490/connectorx/src/sources/postgres/mod.rs#L24

pub fn read_postgres(query: &str, connection_string: &str) -> Result<DataFrame> {
    let mut client = Client::connect(connection_string, NoTls)?;
    let mut data = Vec::new();
    let query = format!("COPY ({}) TO STDOUT WITH BINARY", self.query);
    let reader = client.copy_out(&*query)?;
    let iter = BinaryCopyOutIter::new(reader, &self.pg_schema);
}


// From connector-x
// 
fn fetch_metadata(&mut self) {

    let mut conn = self.pool.get()?;
    let first_query = &self.queries[0];

    let stmt = conn.prepare(first_query.as_str())?;

    let (names, pg_types): (Vec<String>, Vec<postgres::types::Type>) = stmt
        .columns()
        .iter()
        .map(|col| (col.name().to_string(), col.type_().clone()))
        .unzip();

    self.names = names;
    self.schema = pg_types
        .iter()
        .map(PostgresTypeSystem::from)
        .collect();
    self.pg_schema = self
        .schema
        .iter()
        .zip(pg_types.iter())
        .map(|(t1, t2)| PostgresTypePairs(t2, t1).into())
        .collect();
}
