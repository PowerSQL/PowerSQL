use sqlparser::ast::Statement;

use std::env;
use tokio_postgres::{Client, Error, NoTls};
extern crate google_bigquery2 as bigquery2;
extern crate hyper;
extern crate hyper_rustls;
extern crate yup_oauth2 as oauth2;
use bigquery2::{Bigquery, DatasetReference, QueryRequest, QueryResponse};
use oauth2::ServiceAccountAccess;

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

    pub async fn query(&mut self, query: &str) -> Result<i64, String> {
        self.client
            .query(query, &[])
            .await
            .map(|x| x[0].get(0))
            .map_err(|x| format!("Failed to run query {}", x))
    }
}

pub struct BigQueryExecutor {
    hub: Bigquery<hyper::Client, ServiceAccountAccess<hyper::Client>>,
}

impl BigQueryExecutor {
    pub async fn new() -> Result<BigQueryExecutor, String> {
        let key_file = env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .map_err(|_x| "GOOGLE_APPLICATION_CREDENTIALS not provided")?;
        let client_secret = oauth2::service_account_key_from_file(&key_file).unwrap();
        let client = hyper::Client::with_connector(hyper::net::HttpsConnector::new(
            hyper_rustls::TlsClient::new(),
        ));
        let access = oauth2::ServiceAccountAccess::new(client_secret, client);
        let hub = Bigquery::new(
            hyper::Client::with_connector(hyper::net::HttpsConnector::new(
                hyper_rustls::TlsClient::new(),
            )),
            access,
        );
        return Ok(BigQueryExecutor { hub });
    }

    fn build_query(&mut self, query: &str) -> QueryRequest {
        let mut qeury_request = QueryRequest::default();

        qeury_request.query = Some(query.to_string());
        qeury_request.use_legacy_sql = Some(false);
        qeury_request.default_dataset = Some(DatasetReference {
            project_id: Some("website-main".to_string()),
            dataset_id: Some("bla".to_string()),
        });

        return qeury_request;
    }

    fn run_query(&mut self, query: QueryRequest) -> Result<QueryResponse, String> {
        return self
            .hub
            .jobs()
            .query(query, "website-main")
            .doit()
            .map(|(_r, q)| q)
            .map_err(|x| format!("Error {}", x));
    }

    pub async fn execute(&mut self, name: &str, stmt: &Statement) -> Result<(), String> {
        let drop_query = self.build_query(&format!("DROP VIEW IF EXISTS {}", name));
        self.run_query(drop_query)?;

        let query = self.build_query(&format!("{}", stmt));
        self.run_query(query)?;

        Ok(())
    }

    pub async fn query(&mut self, query: &str) -> Result<i64, String> {
        let query = self.build_query(query);
        let res = self.run_query(query)?;
        Ok(res.rows.unwrap()[0].clone().f.unwrap()[0]
            .v
            .as_ref()
            .unwrap()
            .parse::<i64>()
            .unwrap())
    }
}
