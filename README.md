# PowerSQL

[![Join the chat at https://gitter.im/PowerSQL/community](https://badges.gitter.im/PowerSQL/community.svg)](https://gitter.im/PowerSQL/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


<div align="center">
<a href="https://github.com/Dandandan/PowerSQL/actions?query=branch%3Amaster+workflow%3ARust">
<img src="https://github.com/Dandandan/PowerSQL/workflows/Rust/badge.svg?branch=master"/>
</a>
<a href="https://crates.io/crates/powersql">
<img src="https://img.shields.io/crates/v/powersql.svg" />
</a>
</div>

PowerSQL, **the** data transformation tool.

Features:

* Supports many SQL dialects
* Plain SQL helps to integrate with your favourite database tools, formatters and linters: simple add your `CREATE VIEW`  or `CREATE TABLE AS` statements.
* Syntax & type checking avoid errors
* Automatically creates and executes a dependency graph.


## Getting started

Install the latest version using `cargo` (curl https://sh.rustup.rs -sSf | sh).

```
cargo install powersql
```

## PostgreSQL

To get started with PostgreSQL, simply create a new project in a file called `powersql.toml`:

```
[project]
name = "my_project"
models = ["models"]
tests = ["tests]
```

Now create one or more models in the `models` directory:

```sql
CREATE VIEW my_model AS SELECT id, category from my_source;
CREATE TABLE category_stats AS SELECT COUNT(*) category_count FROM my_model GROUP BY category;
```

PowerSQL automatically will create a DAG based on the relations in your database.

To run against the database, provide the following environment variables:

- PG_HOSTNAME
- PG_USERNAME
- PG_PORT
- PG_DATABASE
- PG_PASSWORD

## Commands

- `powersql check`: This will load all your `.sql` files in the directories listed in `models`. It will check the syntax of the SQL statements. After this, it will check the DAG and report if there is a circular dependency. Finally, it will run a type checker and report any type errors.
- `powersql run`: Loads and runs the entire DAG of SQL statements.
- `powersql test`: Loads and runs the data tests.

## Data tests

Data tests are SQL queries that you can run on your database tables and views and perform checks on data quality, recency, etc.
The test fails if the query returns 1 or more rows. 

Some examples:
```sql
-- NULL check
SELECT 1 FROM t WHERE column IS NULL;
-- Check values
SELECT 1 FROM t WHERE amount < 0;
-- Check relations
SELECT 1 FROM t LEFT JOIN u ON t.id = u.id WHERE u.id IS NULL;
-- Prefix check
SELECT 1 FROM t WHERE NOT STARTS_WITH(str_column, "http");

```
