use sqlparser::ast::Statement;

use std::env;
use tokio_postgres::{Client, Error, NoTls};

pub struct PostgresExecutor {
    client: Client,
}

impl PostgresExecutor {
    pub async fn new() -> Result<PostgresExecutor, String> {
        // TODO, simplify, use TLS
        let hostname = env::var("PG_HOSTNAME").map_err(|_x| "PG_HOSTNAME not provided")?;
        let username = env::var("PG_USERNAME").map_err(|_x| "PG_USERNAME not provided")?;
        let port = env::var("PG_PORT").map_err(|_x| "PG_PORT not provided")?;
        let database = env::var("PG_DATABASE").map_err(|_x| "PG_DATABASE not provided")?;
        let password = env::var("PG_PASSWORD").map_err(|_x| "PG_PASSWORD not provided")?;

        let url = format!(
            "postgresql://{username}:{password}@{hostname}:{port}/{database}",
            port = port,
            username = username,
            password = password,
            hostname = hostname,
            database = database,
        );
        let (client, connection) = tokio_postgres::connect(&url, NoTls)
            .await
            .map_err(|_x| "Failed to connect")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        Ok(PostgresExecutor { client })
    }
    pub async fn execute(&mut self, name: &str, stmt: &Statement) -> Result<(), Error> {
        let _ = self
            .client
            .execute(
                format!("DROP VIEW IF EXISTS \"{name}\" CASCADE", name = name,).as_str(),
                &[],
            )
            .await;

        let _ = self
            .client
            .execute(
                format!("DROP TABLE IF EXISTS \"{name}\" CASCADE", name = name,).as_str(),
                &[],
            )
            .await;

        let transaction = self.client.transaction().await?;

        transaction
            .batch_execute(format!("{}", stmt).as_str())
            .await?;

        transaction.commit().await?;

        Ok(())
    }
}
