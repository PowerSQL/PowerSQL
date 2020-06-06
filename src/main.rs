mod execute;
mod parser;
mod types;
use parser::PowerSqlDialect;
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

// TODO don't pass mutable vec
fn get_refs_cte(cte: &Cte, vec: &mut Vec<String>) {
    get_refs(&cte.query, vec)
}

fn get_refs_set_expr(ctes: &SetExpr, vec: &mut Vec<String>) {
    match ctes {
        SetExpr::Query(q) => get_refs(q, vec),
        SetExpr::Select(s) => s.from.iter().for_each(|x| match &x.relation {
            TableFactor::Table { name, .. } => vec.push(name.0.join(".")),
            _ => {}
        }),
        _ => {}
    }
}

fn get_refs(query: &Query, vec: &mut Vec<String>) {
    query.ctes.iter().for_each(|x| get_refs_cte(x, vec));
    get_refs_set_expr(&query.body, vec);
}

fn load_asts(models: &[String]) -> HashMap<String, Query> {
    models
        .par_iter()
        .map(|x| {
            let sql = fs::read_to_string(x).unwrap();

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
                .filter_map(|y| {
                    // TODO handle errors
                    mappings.get(y).map(|s| s.to_string())
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
                    return Err(format!("Loop detected while checking model {}", model));
                }
                visited.insert(i);
            }
            stack.extend(d.clone());
        }
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq, Clone)]
struct ModelNode {
    live_parents: usize,
    next_nodes: Vec<String>,
}

fn build_graph(deps: &HashMap<String, Vec<String>>) -> Result<HashMap<&str, ModelNode>, String> {
    let mut graph = HashMap::new();

    let mut nodes = Vec::new();
    for (model, model_deps) in deps.iter() {
        // for each model collect number of parents
        graph.entry(model.as_str()).or_insert(ModelNode {
            live_parents: 0,
            next_nodes: vec![],
        });

        for m in model_deps {
            nodes.push((model.as_str(), m.as_str()));
        }
    }

    for (to, from) in nodes {
        let x = graph.get_mut(from).unwrap();
        x.next_nodes.push(to.to_string());

        let mut y = graph.get_mut(to).unwrap();
        y.live_parents += 1;
    }

    Ok(graph)
}

fn get_mappings(models: &[String]) -> HashMap<String, String> {
    models
        .iter()
        .flat_map(|x| {
            let parts: Vec<&str> = x.trim_end_matches(".sql").split('/').collect();
            let mut res = Vec::with_capacity(parts.len() - 1);
            let mut items = vec![];
            for &p in parts.iter().rev().take(parts.len()) {
                items.push(p);
                let names: Vec<&str> = items.iter().copied().rev().collect();
                let t = names.join(".");
                res.push((t, x.clone()));
            }
            res
        })
        .collect()
}

#[tokio::main]
pub async fn main() -> Result<(), String> {
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
            let asts = load_asts(&models);

            for (name, query) in &asts {
                let ty = types::get_model_type(query, &im::HashMap::new());
                println!("{} {:?}", name, ty)
            }

            let mappings = get_mappings(&models);
            let dependencies: HashMap<String, Vec<String>> = get_dependencies(&asts, &mappings);

            detect_cycles(&dependencies)?;

            println!("{} models loaded succesfully", asts.len());
        }
        Command::Run => {
            let asts = load_asts(&models);

            let mappings = get_mappings(&models);
            let dependencies: HashMap<String, Vec<String>> = get_dependencies(&asts, &mappings);
            detect_cycles(&dependencies)?;

            let mut graph = build_graph(&dependencies)?;

            let mut nodes: Vec<_> = graph
                .iter()
                .filter(|(_m, node)| node.live_parents == 0)
                .map(|(x, _)| (*x).to_string())
                .collect();
            println!("Graph {:?}", graph);

            let mut executor = execute::PostgresExecutor::new(
                "postgresql://postgres:postgres@localhost:5432/postgres",
            )
            .await
            .map_err(|_x| "Connection error")?;

            while let Some(m) = nodes.pop() {
                println!("Executing {}", m);
                executor
                    .execute(&m, asts.get(&m).unwrap())
                    .await
                    .map_err(|_x| format!("{}", _x))?;
                println!("Ready {}", m);
                println!("Graph {:?}", graph);

                let node = graph.get(m.as_str()).unwrap().clone();
                for n in node.next_nodes.iter() {
                    let mut node = graph.get_mut(n.as_str()).unwrap();
                    node.live_parents -= 1;
                    if node.live_parents == 0 {
                        nodes.push(n.to_string());
                    }
                }
            }
        }
        Command::Lint => unimplemented!(),
        Command::Docs => {
            let asts = load_asts(&models);

            let mappings = get_mappings(&models);
            let dependencies: HashMap<String, Vec<String>> = get_dependencies(&asts, &mappings);

            let arrows: Vec<String> = dependencies
                .iter()
                .flat_map(|(x, y)| y.iter().map(move |z| format!("{z} -> {x}", x = x, z = z)))
                .collect();

            print!("{:?}", arrows);
        }
    }

    Ok(())
}

#[cfg(test)]
#[macro_use]
extern crate maplit;

#[test]
fn test_dependencies() {
    let sql = "select a from t";
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

#[test]
fn test_build_graph() {
    let deps = hashmap! {
        "a".to_string() => vec!["b".to_string()],
        "b".to_string() => vec!["c".to_string()],
        "c".to_string() => vec![],
    };
    let plan = build_graph(&deps).unwrap();

    assert_eq!(
        plan,
        hashmap! {
                "a" => ModelNode {
                    live_parents: 1,
                    next_nodes: vec![]
            },
                "b" => ModelNode {
                    live_parents: 1,
                    next_nodes: vec!["a".to_string()]
            },
                "c" => ModelNode {
                    live_parents: 0,
                    next_nodes: vec!["b".to_string()]
            }
        }
    );
}
