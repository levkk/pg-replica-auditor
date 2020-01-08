# pg-replica-auditor

## Features

### Assumptions
These tests assume that `id` and `updated_at` columns exist and have indexes for efficient querying and that the table exists on both databases.

#### Row comparison
Runs row comparisons between primary and replica using two methods:

1. select 8128 rows at random between `MIN(id)` and `MAX(id)`,
2. select all rows between `MAX(id)` and `MAX(id) - 1000`.

#### Replica lag
Checks for "replica lag" by comparing `MAX(updated_at)` on the given table on both databases.


## Requirements

1. Python 3
2. Postgres development files (required by psycopg2). On Mac OS, use `brew install postgresql`. On Ubuntu, install `libpq-dev`.

## Installation

### Development
Using virtualenv, `pip install -r requirements.txt`

### Production
Using Pypi, `pip install pgreplicaauditor`.

## Usage

This script requires three arguments:
1. `--primary`, any acceptable Postgres connection string (incl. DSN),
2. `--replica`, same as `--primary` but for the replica database,
3. `--table`, the table to check.

Optionally, if you want to see which queries it runs, you can set the `--debug` option.

Example:

```bash
$ pgreplicaauditor --primary=postgres://primary-db.amazonaws.com:5432/my_db --replica=postgres://replica-db.amazonaws.com:5432/my_db --table=immutable_items
```
