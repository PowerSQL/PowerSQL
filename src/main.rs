use rayon::prelude::*;
use serde_derive::Deserialize;
use sqlparser::ast::Query;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace, Word};
use std::collections::HashMap;
use std::fs;
use walkdir::WalkDir;

#[derive(Deserialize, Debug)]
struct PowerSqlConfig {
    project: Project,
}
#[derive(Deserialize, Debug)]
struct Project {
    name: String,
    models: Vec<String>,
}

use structopt::StructOpt;
#[derive(Debug, StructOpt)]
enum Command {
    Check,
    Lint,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "PowerSQL", about = "The data tool")]
struct Opt {
    #[structopt(subcommand, name = "CMD")]
    command: Command,
}

#[derive(Debug)]
struct PowerSqlDialect {}

impl sqlparser::dialect::Dialect for PowerSqlDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        // Ref (@) or normal identifier
        (ch == '@') || (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        // ANSI SQL
        (ch >= 'a' && ch <= 'z')
            || (ch >= 'A' && ch <= 'Z')
            || (ch >= '0' && ch <= '9')
            || ch == '_'
    }
}

pub fn main() -> Result<(), String> {
    let opt = Opt::from_args();

    // Load project
    let contents = fs::read_to_string("powersql.toml").expect("No powersql.toml file found");
    let config = toml::from_str(&contents);
    let config: PowerSqlConfig = match config {
        Err(x) => return Err(x.to_string()),
        Ok(conf) => conf,
    };

    let dialect = PowerSqlDialect {};
    let mut models = vec![];

    for dir in config.project.models {
        for entry in WalkDir::new(format!("{}", dir)) {
            let entry = entry.unwrap();
            if let Some(ext) = entry.path().extension() {
                {
                    if ext == "sql" {
                        let e = entry.clone();
                        models.push(e);
                    }
                }
            }
        }
    }

    match opt.command {
        Command::Check => {
            // Check / load
            let asts: HashMap<String, Query> = models
                .par_iter()
                .map(|x| {
                    let sql = fs::read_to_string(x.path()).unwrap();

                    //let ast = Parser::parse_sql(&dialect, sql).unwrap();
                    let tokens = Tokenizer::new(&dialect, &sql).tokenize().unwrap();
                    let mut parser = Parser::new(tokens);
                    let ast = parser.parse_query().unwrap();

                    (x.path().to_str().unwrap().to_string(), ast)
                })
                .collect();

            println!("{} models correctly loaded", asts.len());
        }
        Command::Lint => unimplemented!(),
    }

    Ok(())
}
