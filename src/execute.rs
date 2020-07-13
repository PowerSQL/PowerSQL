use sqlparser::ast::Statement;

use std::collections::HashMap;
use std::env;
use tokio_postgres::{Client, Error, NoTls, Row};
extern crate google_bigquery2 as bigquery2;
extern crate hyper;
extern crate hyper_rustls;
extern crate yup_oauth2 as oauth2;
use bigquery2::{Bigquery, QueryRequest};
use oauth2::{
    ApplicationSecret, Authenticator, DefaultAuthenticatorDelegate, MemoryStorage,
    ServiceAccountAccess,
};
use std::fs;
use std::path::Path;

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

    pub async fn query(&mut self, query: &str) -> Result<Vec<Row>, String> {
        self.client
            .query(query, &[])
            .await
            .map_err(|x| format!("Failed to run query {}", x))
    }
}

pub struct BigQueryExecutor {
    hub: Bigquery<hyper::Client, ServiceAccountAccess<hyper::Client>>,
}

impl BigQueryExecutor {
    pub async fn new() -> Result<BigQueryExecutor, String> {
        //let mut secret: ApplicationSecret = ApplicationSecret::default();
        let client_secret =
            yup_oauth2::service_account_key_from_file(&"application.json".to_string()).unwrap();
        let client = hyper::Client::with_connector(hyper::net::HttpsConnector::new(
            hyper_rustls::TlsClient::new(),
        ));
        let access = yup_oauth2::ServiceAccountAccess::new(client_secret, client);
        let hub = Bigquery::new(
            hyper::Client::with_connector(hyper::net::HttpsConnector::new(
                hyper_rustls::TlsClient::new(),
            )),
            access,
        );
        return Ok(BigQueryExecutor { hub });
    }

    pub async fn query(&mut self, query: &str) -> Result<Vec<Row>, String> {
        let mut req = QueryRequest::default();
        req.query = Some(query.to_string());
        let res = self
            .hub
            .jobs()
            .query(req, "website-main")
            .doit()
            .map_err(|x| format!("Error {}", x));
        println!("{:?}", res);
        Ok(vec![])
    }
}
