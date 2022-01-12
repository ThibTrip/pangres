[![CircleCI](https://circleci.com/gh/ThibTrip/pangres.svg?style=svg&circle-token=3e39be6b969ed02b41d259c279da0d9e63751506)](https://circleci.com/gh/ThibTrip/pangres) [![codecov](https://codecov.io/gh/ThibTrip/pangres/branch/master/graph/badge.svg)](https://codecov.io/gh/ThibTrip/pangres) [![PyPI version](https://img.shields.io/pypi/v/pangres)](https://img.shields.io/pypi/v/pangres)

# pangres
![pangres logo](logo.png)

_Thanks to [freesvg.org](https://freesvg.org/) for the logo assets_

Upsert with pandas DataFrames (<code>ON CONFLICT DO NOTHING</code> or <code>ON CONFLICT DO UPDATE</code>) for PostgreSQL, MySQL, SQlite and potentially other databases behaving like SQlite (untested) with some additional optional features (see features). Upserting can be done with **primary keys** or **unique keys**.
Pangres also handles the creation of non existing SQL tables and schemas.


# Features

1. <i>(optional)</i> Automatical column creation (when a column exists in the DataFrame but not in the SQL table).
2. <i>(optional)</i> Automatical column type alteration for columns that are empty in the SQL table (except for SQlite where alteration is limited).
3. Creates the table if it is missing.
4. Creates missing schemas in Postgres (and potentially other databases that have a schema system).
5. JSON is supported (with pd.to_sql it does not work) with some exceptions (see [Gotchas and caveats](#Gotchas-and-caveats)).
6. Fast (except for SQlite where some help is needed).
7. Will work even if not all columns defined in the SQL table are there.
8. SQL injection safe (schema, table and column names are escaped and values are given as parameters).

# Tested with
* Python 3.7.3 and Python 3.8.0
* MySQL 5.7.29 using pymysql 0.9.3
* PostgreSQL 9.6.17 using psycopg2 2.8.4
* SQlite 3.28.0 using sqlite3 2.6.0

# Gotchas and caveats

## All flavors
1. We can't create JSON columns automatically but we can insert JSON like objects (list, dict) in existing JSON columns.

## Postgres

1. "%", ")" and "(" in column names will most likely cause errors with PostgreSQL (this is due to psycopg2 and also affect pd.to_sql). Use the function pangres.fix_psycopg2_bad_cols to "clean" the columns in the DataFrame. You'll also have to rename columns in the SQL table accordingly (if the table already exists).
2. Even though we only do data type alteration on empty columns, since we don't want to lose column information (e.g. constraints) we use true column alteration (instead of drop+create) so the old data type must be castable to the new data type. Postgres seems a bit restrictive in this regard even when the columns are empty (e.g. BOOLEAN to TIMESTAMP is impossible).

## SQlite
1. **SQlite must be version 3.24.4 or higher**! UPSERT syntax did not exist before. 
2. Column type alteration is not possible for SQlite.
3. SQlite inserts can be at worst 5 times slower than pd.to_sql for some reasons. If you can help please contact me!
4. Inserts with 1000 columns (or 32767 columns for SQlite >= 3.32.0) or more are not supported because one could not even insert one row without exceeding the max number of parameters per queries. One way to fix this would inserting the columns progressively but this seems quite tricky. If you know a better way please contact me.

## MySQL

1. MySQL will often change the order of the primary keys in the SQL table when using INSERT... ON CONFLICT.. DO NOTHING/UPDATE. This seems to be the expected behavior so nothing we can do about it but please mind that!
2. You may need to provide SQL dtypes e.g. if you have a primary key with text you will need to provide a character length (e.g. VARCHAR(50)) because MySQL does not support indices/primary keys with flexible text length. pd.to_sql has the same issue.


# Notes

This is a library I was using in production in private with very good results and decided to publish.

Ideally such features will be integrated into pandas since there is already a [PR on the way](https://github.com/pandas-dev/pandas/pull/29636)) and I would like to give the option to add columns via another PR.

There is also [pandabase](https://github.com/notsambeck/pandabase) which does almost the same thing (plus lots of extra features) but my implementation is different.
Btw big thanks to pandabase and the sql part of pandas which helped a lot.

# Installation
```
pip install pangres
```
Additionally depending on which database you want to work with you will need to install the corresponding library (note that SQlite is included in the standard library):

* Postgres
```
pip install psycopg2
```

* MySQL
```
pip install pymysql
```

# Usage
Head over to [pangres' wiki](https://github.com/ThibTrip/pangres/wiki)!

# Contributing

Pull requests/issues are welcome.

Note: I develop the library inside of Jupyter Lab using the [jupytext](https://github.com/mwouts/jupytext) extension.
I recommand using this extension for the best experience. It will split code blocks within modules in cells and will help thanks to interactive development.
If you wish you can also use the provided environment (see `environment.yml` file) inside of Jupyter Lab/Notebook thanks to [nb_conda_kernels](https://github.com/Anaconda-Platform/nb_conda_kernels).

# Testing

You can test one or multiple of the following SQL flavors (you will of course need a live database for this): PostgreSQL, SQlite or MySQL.

NOTE: in one of the tests of `pangres` we will try to drop and then create a PostgreSQL schema called `pangres_create_schema_test`. If the schema existed and was not empty an error will be raised.

Clone pangres then set your curent working directory to the root of the cloned repository folder. Then use the commands below. You will have to replace the following variables in those commands:
* SQLITE_CONNECTION_STRING: replace with a SQlite sqlalchemy connection string (e.g. "sqlite:///test.db")
* POSTGRES_CONNECTION_STRING: replace with a Postgres sqlalchemy connection string (e.g. "postgres:///user:password@localhost:5432/database"). Specifying schema is optional for postgres (will default to public).
* PG_SCHEMA (optional): schema for postgres (defaults to public)
* MYSQL_CONNECTION_STRING: replace with a MySQL sqlalchemy connection string (e.g. "mysql+pymysql:///user:password@localhost:3306/database")

```shell
# 1. Create and activate the build environment
conda env create -f environment.yml
conda activate pangres-dev
# 2. Install pangres in editable mode (changes are reflected upon reimporting)
pip install -e .
# 3. Run pytest
# -s prints stdout
# -v prints test parameters
# --cov=./pangres shows coverage only for pangres
# --doctest-modules tests with doctest in all modules
# --benchmark-XXX : these are options for benchmarks tests (see https://pytest-benchmark.readthedocs.io/en/latest/usage.html)
pytest -s -v pangres --cov=pangres --doctest-modules --sqlite_conn=$SQLITE_CONNECTION_STRING --pg_conn=$POSTGRES_CONNECTION_STRING --mysql_conn=$MYSQL_CONNECTION_STRING --pg_schema=tests --benchmark-group-by=func,param:engine,param:nb_rows --benchmark-columns=min,max,mean,rounds --benchmark-sort=name --benchmark-name=short
```

Additionally, the following flags could be of interest for you:
* `-x` for stopping at the first failure
* `--benchmark-only` for only testing benchmarks
* `--benchmark-skip` for skipping benchmarks