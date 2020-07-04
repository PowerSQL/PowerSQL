# PowerSQL


<div align="center">
<a href="https://github.com/Dandandan/PowerSQL/actions?query=branch%3Amaster+workflow%3ARust">

<img src="https://github.com/Dandandan/PowerSQL/workflows/Rust/badge.svg?branch=master"/>
</a>
</div>

PowerSQL, **the** data transformation tool.

Features:

* Supports many SQL dialects
* Plain SQL helps to integrate with your favourite database tools, formatters and linters: simple add your `CREATE VIEW`  or `CREATE TABLE AS` statements.
* Syntax & type checking avoid errors
* Automatically creates and executes a dependency graph.


## Get started

Clone repository and install using `cargo`.

```
cargo install --path .
```

## PostgreSQL

To get started with PostgreSQL, simply create a new project in a file called `powersql.toml`:

```
[project]
name = "my_project"
models = ["models"]
```

To run against the database, provide the following environment variables:

- PG_HOSTNAME
- PG_USERNAME
- PG_PORT
- PG_DATABASE
- PG_PASSWORD
