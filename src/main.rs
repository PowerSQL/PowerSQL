use rayon::prelude::*;
use serde_derive::Deserialize;
use sqlparser::ast::{Cte, Query, SetExpr, TableFactor};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Tokenizer;
use std::collections::HashMap;
use std::collections::HashSet;
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
    Run,
    Lint,
    Docs,
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

// TODO don't pass mutable vec
fn get_refs_cte(cte: &Cte, vec: &mut Vec<String>) {
    get_refs(&cte.query, vec)
}

fn get_refs_set_expr(ctes: &SetExpr, vec: &mut Vec<String>) {
    match ctes {
        SetExpr::Query(q) => get_refs(q, vec),
        SetExpr::Select(s) => s.from.iter().for_each(|x| match &x.relation {
            TableFactor::Table { name, .. } => {
                if name.0[0].starts_with("@") {
                    vec.push(name.0.join("."))
                }
            }
            _ => {}
        }),
        _ => {}
    }
}

fn get_refs(query: &Query, vec: &mut Vec<String>) {
    query.ctes.iter().for_each(|x| get_refs_cte(x, vec));
    get_refs_set_expr(&query.body, vec);
}

fn load_asts(models: &Vec<String>) -> HashMap<String, Query> {
    models
        .par_iter()
        .map(|x| {
            let sql = fs::read_to_string(x).unwrap();

            //let ast = Parser::parse_sql(&dialect, sql).unwrap();
            let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
                .tokenize()
                .unwrap();
            let mut parser = Parser::new(tokens);
            // TODO handle parse error
            let ast = parser.parse_query().unwrap();

            (x.clone(), ast)
        })
        .collect()
}

fn get_dependencies(
    asts: &HashMap<String, Query>,
    mappings: &HashMap<String, String>,
) -> HashMap<String, Vec<String>> {
    asts.iter()
        .map(|(src, query)| {
            let mut x = vec![];
            get_refs(query, &mut x);
            let m = x
                .iter()
                .map(|y| {
                    // TODO handle errors
                    let s = y.trim_start_matches('@');
                    (*mappings.get(s).unwrap()).to_string()
                })
                .collect();
            (src.clone(), m)
        })
        .collect()
}

fn detect_cycles(deps: &HashMap<String, Vec<String>>) -> Result<(), String> {
    let mut visited_all = HashSet::new();
    for (model, model_deps) in deps.iter() {
        let mut visited = HashSet::new();
        let mut stack = model_deps.clone();

        visited.insert(model);
        while let Some(x) = stack.pop() {
            if visited_all.contains(&x) {
                continue;
            }
            visited_all.insert(x.clone());

            let d = deps.get(&x).ok_or(format!("Model {} not found", &x))?;

            for i in d.iter() {
                if visited.contains(i) {
                    println!("{}", i);
                    return Err(format!("Loop detected while checking model {}", model));
                }
                visited.insert(i);
            }
            stack.extend(d.clone());
        }
    }
    Ok(())
}

fn get_mappings(models: &Vec<String>) -> HashMap<String, String> {
    models
        .iter()
        .flat_map(|x| {
            let parts: Vec<&str> = x.trim_end_matches(".sql").split('/').collect();
            let mut res = Vec::with_capacity(parts.len() - 1);
            let mut items = vec![];
            for &p in parts.iter().rev().take(parts.len()) {
                items.push(p);
                let names: Vec<&str> = items.iter().map(|x| *x).rev().collect();
                let t = names.join(".");
                res.push((t, x.clone()));
            }
            res
        })
        .collect()
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
    let mut models = vec![];

    for dir in config.project.models {
        for entry in WalkDir::new(dir.to_string()) {
            let entry = entry.unwrap();
            if let Some(ext) = entry.path().extension() {
                {
                    if ext == "sql" {
                        models.push(entry.path().to_str().unwrap().to_string());
                    }
                }
            }
        }
    }

    match opt.command {
        Command::Check => {
            // Create mappings to full name
            let mappings = get_mappings(&models);
            let asts = load_asts(&models);

            // Create list of dependencies
            let deps: HashMap<String, Vec<String>> = get_dependencies(&asts, &mappings);

            // Cycle detection
            detect_cycles(&deps)?;

            println!("{:?}", mappings);
            println!("{} models loaded", asts.len());
            println!("{:?} deps", deps);
        }
        Command::Run => {}
        Command::Lint => unimplemented!(),
        Command::Docs => unimplemented!(),
    }

    Ok(())
}

#[cfg(test)]
#[macro_use]
extern crate maplit;

#[test]
fn test_dependencies() {
    let sql = "select a from @t";
    let tokens = Tokenizer::new(&PowerSqlDialect {}, &sql)
        .tokenize()
        .unwrap();
    let mut parser = Parser::new(tokens);
    let ast = parser.parse_query().unwrap();

    let x = get_dependencies(
        &(vec![("x".to_string(), ast)].iter().cloned().collect()),
        &(vec![("t".to_string(), "t.sql".to_string())]
            .iter()
            .cloned()
            .collect()),
    );

    assert_eq!(x, hashmap! {"x".to_string() => vec!["t.sql".to_string()]})
}

#[test]
fn test_cycle_detection_err() {
    assert!(matches!(
        detect_cycles(&hashmap! {
            "a".to_string() => vec!["b".to_string()],
            "b".to_string() => vec!["a".to_string()]
        }),
        Err(_)
    ));
}

#[test]
fn test_cycle_detection_err_not_found() {
    assert!(matches!(
        detect_cycles(&hashmap! {
            "a".to_string() => vec!["b".to_string()],
            "b".to_string() => vec!["c".to_string()]
        }),
        Err(_)
    ));
}

#[test]
fn test_cycle_detection_ok() {
    assert!(matches!(
        detect_cycles(&hashmap! {
            "a".to_string() => vec!["b".to_string()],
            "b".to_string() => vec![]
        }),
        Ok(_)
    ));
}

#[test]

fn test_mappings() {
    let models = vec!["a/b/c.sql".to_string()];
    let mappings = get_mappings(&models);

    assert_eq!(
        mappings,
        hashmap! {"c".to_string() => "a/b/c.sql".to_string(),
            "b.c".to_string() => "a/b/c.sql".to_string(),
            "a.b.c".to_string() => "a/b/c.sql".to_string()
        }
    )
}
