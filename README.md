# pg-replica-checksummer

## Installation
Using virtualenv, `pip install -r requirements.txt`

## Usage

This script requires three arguments:
1. `--primary`, any acceptable Postgres connection string (incl. DSN),
2. `--replica`, same as `--primary` but for the replica database,
3. `--table`, the table to check.

Optionally, if you want to see which queries it runs, you can set the `--debug` option.

```bash
$ (venv) python checksummer.py --primary=postgres://primary-db.amazonaws.com:5432/my_db --replica=postgres://replica-db.amazonaws.com:5432/my_db --table=immutable_items
```