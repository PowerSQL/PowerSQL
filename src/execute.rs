use sqlparser::ast::Query;

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

        Ok(PostgresExecutor { client: client })
    }
    pub async fn execute(&mut self, name: &str, query: &Query) -> Result<(), Error> {
        let transaction = self.client.transaction().await?;
        println!("{}", query);

        // let drop = Statement::Drop {
        //     object_type: ObjectType::View,
        //     if_exists: true,
        //     names: vec![ObjectName(vec![name.to_string()])],
        //     cascade: false,
        // };
        // let create = Statement::CreateView {
        //     name: ObjectName(vec![name.to_string()]),
        //     columns: vec![],
        //     query: Box::new(query.clone()),
        //     materialized: false,
        //     with_options: vec![],
        // };

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
