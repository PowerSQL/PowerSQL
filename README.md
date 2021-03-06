# PowerSQL


<div align="center">
<a href="https://github.com/Dandandan/PowerSQL/actions?query=branch%3Amaster+workflow%3ATests">
<img src="https://github.com/Dandandan/PowerSQL/workflows/Tests/badge.svg?branch=master"/>
</a>
<a href="https://crates.io/crates/powersql">
<img src="https://img.shields.io/crates/v/powersql.svg" />
</a>
<a href="https://gitter.im/PowerSQL/community">
<img src="https://badges.gitter.im/PowerSQL/community.svg" />
</a>
<a href="https://powersql.github.io">
<img src="https://img.shields.io/badge/Docs-PowerSQL-blue" />
</a>
</div>

PowerSQL, **the** data transformation tool for data {engineers, scientists, analysts}.

PowerSQL automatically find the relations between your SQL statements and runs your transformations as a job on your database / data warehouse / data engine.

Features:

* Supports many SQL dialects (BigQuery and PostgreSQL now supported to execute queries on).
* Supports plain SQL to make it easy to integrate with your favourite database tools, formatters and linters: simple add your `CREATE [MATERIALIZED] VIEW` , `CREATE TABLE AS` statements.
* Syntax & type checking avoids errors early on
* Automatically creates and executes a dependency graph.
* Perform automated data testing using simple SQL queries


## Getting started

Install the latest version using `cargo` (`curl https://sh.rustup.rs -sSf | sh`).

```bash
# For PostgreSQL
cargo install powersql --features postgres
# For BigQuery
cargo install powersql --features bigquery
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

## BigQuery

To run against the database, provide the following environment variables:

- GOOGLE_APPLICATION_CREDENTIALS
- PROJECT_ID
- DATASET_ID
- LOCATION

`GOOGLE_APPLICATION_CREDENTIALS` should refer to an service account key file (this can be set by an appliation rather than locally).

`PROJECT_ID` is the id (not number) of the project and `DATASET_ID` is the name of the dataset that is used by default.

`LOCATION` is an (optional) datacenter location id where the query is being executed.

## Commands

- `powersql check`: This will load all your `.sql` files in the directories listed in `models`. It will check the syntax of the SQL statements. After this, it will check the DAG and report if there is a circular dependency. Finally, it will run a type checker and report any type errors.
- `powersql run`: Loads and runs the entire DAG of SQL statements.
- `powersql test`: Loads and runs the data tests. By running `powersql test --fail-fast` powersql will stop at the first failure.

## Data tests

Data tests are `ASSERT` statements that you can run on your database tables and views and perform checks on data quality, recency, etc. Assert statements checks the result of a condition - a boolean expression.
Assert-based testing are enabled for every backend, they are translated by PowerSQL to queries return a
boolean.

Some examples:
```sql
-- Column should be NOT NULL
ASSERT NOT EXISTS(
  SELECT X
  FROM t
  WHERE column IS NULL
) AS 'column should be non null';
ASSERT NOT EXISTS (
    SELECT quantity
    FROM rev_per_product
    WHERE quantity <= 0
) AS 'quantity should be positive';
ASSERT NOT EXISTS (
    SELECT product_id
    FROM rev_per_product
    WHERE product_id IS NULL
) AS 'product_id should be not null';
ASSERT (
    SELECT COUNT (*)
    FROM rev_per_product
    WHERE quantity < 10
) >= 0.7 * (    
    SELECT COUNT(*)
    FROM rev_per_product
) AS 'At least 70% should have a quantity lower than 10'
```
