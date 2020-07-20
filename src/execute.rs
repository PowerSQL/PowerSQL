use sqlparser::ast::Statement;

use std::env;
#[cfg(feature = "postgres")]
use tokio_postgres::{types, Client, NoTls};
#[cfg(feature = "bigquery")]
extern crate google_bigquery2 as bigquery2;
#[cfg(feature = "bigquery")]
extern crate hyper;
#[cfg(feature = "bigquery")]
extern crate hyper_rustls;
#[cfg(feature = "bigquery")]
extern crate yup_oauth2 as oauth2;
use async_trait::async_trait;
#[cfg(feature = "bigquery")]
use bigquery2::{Bigquery, DatasetReference, Error, QueryRequest, QueryResponse};
#[cfg(feature = "bigquery")]
use oauth2::ServiceAccountAccess;

#[async_trait]
pub trait Executor {
    async fn new() -> Result<Self, String>
    where
        Self: Sized;
    async fn execute(&mut self, name: &str, stmt: &Statement) -> Result<(), String>;
    async fn execute_raw(&mut self, stmt: &Statement) -> Result<(), BackendError>;
    async fn query_bool(&mut self, query: &str) -> Result<bool, String>;
}

pub enum BackendError {
    Message { message: String },
}

impl BackendError {
    fn get_message(self) -> String {
        match self {
            BackendError::Message { message } => message,
        }
    }
}

#[cfg(feature = "postgres")]
pub struct Postgres {
    client: Client,
}

#[async_trait]
#[cfg(feature = "postgres")]
impl Executor for Postgres {
    async fn new() -> Result<Postgres, String> {
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

        Ok(Postgres { client })
    }
    async fn execute(&mut self, name: &str, stmt: &Statement) -> Result<(), String> {
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

        let transaction = self
            .client
            .transaction()
            .await
            .map_err(|e| format!("PostgresError {}", e))?;

        transaction
            .batch_execute(format!("{}", stmt).as_str())
            .await
            .map_err(|e| format!("PostgresError {}", e))?;

        transaction
            .commit()
            .await
            .map_err(|e| format!("PostgresError {}", e))?;

        Ok(())
    }

    async fn execute_raw(&mut self, stmt: &Statement) -> Result<(), BackendError> {
        let _ = self
            .client
            .execute(format!("{}", stmt).as_str(), &[])
            .await
            .map_err(|x| BackendError::Message {
                message: format!("{}", x),
            })?;
        Ok(())
    }

    async fn query_bool(&mut self, query: &str) -> Result<bool, String> {
        self.client
            .query(query, &[])
            .await
            .map(|x| x[0].get(0))
            .map_err(|x| format!("Failed to run query {}", x))
    }
}
#[cfg(feature = "bigquery")]
pub struct BigqueryRunner {
    hub: Bigquery<hyper::Client, ServiceAccountAccess<hyper::Client>>,
    dataset_id: String,
    project_id: String,
    location: Option<String>,
}

#[cfg(feature = "bigquery")]
impl BigqueryRunner {
    fn build_query(&mut self, query: &str) -> QueryRequest {
        let mut query_request = QueryRequest::default();

        query_request.query = Some(query.to_string());
        query_request.use_legacy_sql = Some(false);
        query_request.location = self.location.clone();
        query_request.default_dataset = Some(DatasetReference {
            project_id: Some(self.project_id.to_string()),
            dataset_id: Some(self.dataset_id.to_string()),
        });

        return query_request;
    }

    fn run_query(&mut self, query: QueryRequest) -> Result<QueryResponse, BackendError> {
        return self
            .hub
            .jobs()
            .query(query, &self.project_id)
            .doit()
            .map(|(_r, q)| q)
            .map_err(|x| match x {
                _ => BackendError::Message {
                    message: format!("{}", x),
                },
            });
    }
}

#[cfg(feature = "bigquery")]
#[async_trait]
impl Executor for BigqueryRunner {
    async fn new() -> Result<BigqueryRunner, String> {
        let key_file = env::var("GOOGLE_APPLICATION_CREDENTIALS")
            .map_err(|_x| "GOOGLE_APPLICATION_CREDENTIALS not provided")?;

        let project_id = env::var("PROJECT_ID").map_err(|_x| "PROJECT_ID not provided")?;
        let dataset_id = env::var("DATASET_ID").map_err(|_x| "DATASET_ID not provided")?;
        let location = env::var("LOCATION").ok();

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
        return Ok(BigqueryRunner {
            hub,
            project_id,
            dataset_id,
            location,
        });
    }

    async fn execute_raw(&mut self, stmt: &Statement) -> Result<(), BackendError> {
        let query = self.build_query(&format!("{}", stmt));
        self.run_query(query)?;
        Ok(())
    }

    async fn execute(&mut self, name: &str, stmt: &Statement) -> Result<(), String> {
        // TODO use CREATE OR REPLACE
        let drop_query = self.build_query(&format!("DROP VIEW IF EXISTS {}", name));
        let _ = self.run_query(drop_query);
        let drop_query = self.build_query(&format!("DROP TABLE IF EXISTS {}", name));
        let _ = self.run_query(drop_query);

        let query = self.build_query(&format!("{}", stmt));
        self.run_query(query).map_err(|x| x.get_message())?;

        Ok(())
    }

    async fn query_bool(&mut self, query: &str) -> Result<bool, String> {
        let query = self.build_query(query);
        let res = self.run_query(query).map_err(|x| x.get_message())?;
        Ok(res.rows.unwrap()[0].clone().f.unwrap()[0]
            .v
            .as_ref()
            .unwrap()
            .parse::<bool>()
            .unwrap())
    }
}
