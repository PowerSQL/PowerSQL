use sqlparser::ast::Query;

use tokio_postgres::{Error, NoTls};

#[derive(Copy, Clone)]
pub struct PostgresExecutor {}

impl PostgresExecutor {
    pub async fn execute(self, name: &str, query: &Query) -> Result<(), Error> {
        println!("Making connection");
        let (mut client, connection) = tokio_postgres::connect(
            "postgresql://postgres:postgres@localhost:5432/postgres",
            NoTls,
        )
        .await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        let transaction = client.transaction().await?;
        println!("{}", query);
        transaction
            .batch_execute(
                format!(
                    "DROP VIEW IF EXISTS \"{name}\";
                    CREATE VIEW \"{name}\" AS ({query})",
                    name = name,
                    query = query
                )
                .as_str(),
            )
            .await?;

        transaction.commit().await?;

        Ok(())
    }
}
