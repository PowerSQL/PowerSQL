use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use walkdir::{Error, WalkDir};

use serde_derive::Deserialize;

#[derive(Deserialize, Debug)]
struct PowerSqlConfig {
    project: Project,
}
#[derive(Deserialize, Debug)]
struct Project {
    name: String,
    models: Vec<String>,
}

pub fn main() -> Result<(), Error> {
    let root_dir = "examples/project_1/";

    // Load project
    let contents = fs::read_to_string(format!("{}{}", root_dir, "powersql.toml"))
        .expect("No powersql.toml file found");

    let config: PowerSqlConfig = toml::from_str(&contents).unwrap();
    println!("{:?}", config);

    let dialect = AnsiDialect {};

    let mut models = vec![];

    for dir in config.project.models {
        for entry in WalkDir::new(format!("{}/{}", root_dir, dir)) {
            let entry = entry.unwrap();
            if let Some(abc) = entry.path().extension() {
                {
                    if abc == "sql" {
                        let e = entry.clone();
                        models.push(e);
                    }
                }
            }
        }
    }

    for m in models {
        let sql = fs::read_to_string(m.path()).unwrap();

        let ast = Parser::parse_sql(&dialect, sql).unwrap();

        println!("AST: {:?}", ast);
    }

    Ok(())
}
