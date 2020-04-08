# pg-replica-auditor

A tool to compare a PostgreSQL logical replica to its primary. It can help detect data inconsistencies.

## Features

### Assumptions
These tests assume that `id` and `updated_at` (or column specified in `--lag-column`) columns exist and have indexes for efficient querying and that the table exists on both databases.

#### Row comparison
Runs row comparisons between primary and replica using two methods:

1. select 8128 rows (or number of rows given to `--rows`) at random between `MIN(id)` and `MAX(id)`
2. select the last 1000 rows.

#### Replica lag
Checks for "replica lag" by comparing `MAX(updated_at)` on the given table on both databases.

#### MinMax
Checks that the minimum `id` and the maximum `id` match on both replica and primary. These can drift _a little_ because of replica lag.

#### Bulk 1000 Sum
Take the sum of the `id` column in chunks of 1000 and compare it between databases. This assumes that retrieving rows in bulk is easier than at random and runs faster than the row comparison and can scan more rows.

#### Count all rows
Counts all the rows using `COUNT(lag_column)` to make sure row counts match on both replica and primary. Very slow, since it has to do a full scan (index or table). Adjust `--count-before` to count all columns before a timestamp on `--lag-column`, or `updated_at` by default.

#### Missing Sequential Records
Go throught the table with a step size of `MAX(id)` * `--step-size=0.01`. The assumption is that if records will be missing, they will be missing in bulk, grouped together.

## Requirements

1. Python 3
2. Postgres development files (required by psycopg2). On Mac OS, use `brew install postgresql`. On Ubuntu, install `libpq-dev`.

## Installation

### Development
Using virtualenv, `pip install -r requirements.txt`

### Production
Using Pypi, `pip install pg-replica-auditor`.

## Usage

This script requires three arguments:
1. `--primary`, any acceptable Postgres connection string (incl. DSN),
2. `--replica`, same as `--primary` but for the replica database,

Optional arguments:
1. `--exclude-tables`, excludes the comma-separated tables from the scan,
2. `--table`, only scans this table,
3. `--debug`, will print debugging information,
4. `--rows`, will scan this many rows in the row comparisons check,
5. `--lag-column`, will use this column for the replica lag check,
6. `--show-skipped`, will print the skipped rows in the Last 1000 check,
7. `--count-before`, will count all rows in the table created/updated before this timestamp,
8. `--step-size`, will decrease the step size for missing sequential records search.

Example:

```bash
$ pgreplicaauditor --primary=postgres://primary-db.amazonaws.com:5432/my_db --replica=postgres://replica-db.amazonaws.com:5432/my_db --table=immutable_items --lag-column=created_at --count-before="2020-04-06"
```
