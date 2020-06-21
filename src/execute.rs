use sqlparser::ast::Statement;

use tokio_postgres::{Client, Error, NoTls};

pub struct PostgresExecutor {
    client: Client,
}

impl PostgresExecutor {
    pub async fn new(url: &str) -> Result<PostgresExecutor, Error> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(PostgresExecutor { client })
    }
    pub async fn execute(&mut self, name: &str, stmt: &Statement) -> Result<(), Error> {
        let transaction = self.client.transaction().await?;
        println!("{}", stmt);

        transaction
            .batch_execute(
                format!(
                    "DROP VIEW IF EXISTS \"{name}\" CASCADE;
                     {stmt}",
                    name = name,
                    stmt = stmt
                )
                .as_str(),
            )
            .await?;

        transaction.commit().await?;

        Ok(())
    }
}
