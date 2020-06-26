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
        let is_table = self
            .client
            .execute(
                "SELECT count(*) FROM pg_tables where tablename='model'",
                &[],
            )
            .await?;

        let view_or_table = if is_table == 1 { "TABLE" } else { "VIEW" };
        let with_local = if is_table == 1 { "" } else { "WITH LOCAL" };

        let transaction = self.client.transaction().await?;

        transaction
            .batch_execute(
                format!(
                    "DROP {view_or_table} IF EXISTS \"{name}\" CASCADE {with_local};
                     {stmt}",
                    view_or_table = view_or_table,
                    with_local = with_local,
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
