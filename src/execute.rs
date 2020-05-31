use sqlparser::ast::Query;

use tokio_postgres::{Error, NoTls};

#[derive(Copy, Clone)]
pub struct PostgresExecutor {}

impl PostgresExecutor {
    pub async fn execute(self, name: &str, query: &Query) -> Result<(), Error> {
        print!("Making connection");
        let (client, connection) = tokio_postgres::connect(
            "postgresql://postgres:postgres@localhost:5432/postgres",
            NoTls,
        )
        .await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        client
            .query(format!("DROP VIEW IF EXISTS \"{}\"", name).as_str(), &[])
            .await?;
        client
            .query(
                format!("CREATE OR REPLACE VIEW \"{}\" AS ({})", name, query).as_str(),
                &[],
            )
            .await?;
        println!();

        Ok(())
    }
}
