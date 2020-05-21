use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser;
use std::fs;
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

    for dir in config.project.models {
        for entry in WalkDir::new(format!("{}/{}", root_dir, dir)) {
            let entry = entry?;
            if let Some(abc) = entry.path().extension() {
                {
                    if abc == "sql" {
                        println!("{:?}", entry.path());
                    }
                }
            }
        }
    }

    let sql = "SELECT a, b, 123, myfunc(b) \
           FROM table_1 \
           WHERE a > b AND b < 100 \
           ORDER BY a DESC, b";

    let ast = Parser::parse_sql(&dialect, sql.to_string()).unwrap();

    println!("AST: {:?}", ast);

    Ok(())
}
