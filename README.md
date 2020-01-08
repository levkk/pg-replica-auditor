# pg-replica-checksummer

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