[![CircleCI](https://circleci.com/gh/ThibTrip/pangres.svg?style=svg&circle-token=3e39be6b969ed02b41d259c279da0d9e63751506)](https://circleci.com/gh/ThibTrip/pangres)
[![codecov](https://codecov.io/gh/ThibTrip/pangres/branch/master/graph/badge.svg)](https://codecov.io/gh/ThibTrip/pangres)
[![PyPI version](https://img.shields.io/pypi/v/pangres)](https://img.shields.io/pypi/v/pangres)
[![Documentation](https://img.shields.io/badge/wiki-documentation-forestgreen)](https://github.com/ThibTrip/pangres/wiki)
[![Made withJupyter](https://img.shields.io/badge/Made%20with-Jupyter-orange?logo=Jupyter)](https://jupyter.org/try)

# pangres
![pangres logo](https://raw.githubusercontent.com/ThibTrip/pangres/master/logo.png)

_Thanks to [freesvg.org](https://freesvg.org/) for the logo assets_

Upsert with pandas DataFrames (<code>ON CONFLICT DO NOTHING</code> or <code>ON CONFLICT DO UPDATE</code>) for PostgreSQL, MySQL, SQlite and potentially other databases behaving like SQlite (untested) with some additional optional features (see features). Upserting can be done with **primary keys** or **unique keys**.
Pangres also handles the creation of non-existing SQL tables and schemas.


# Features

1. <i>(optional)</i> Automatical column creation (when a column exists in the DataFrame but not in the SQL table)
2. <i>(optional)</i> Automatical column type alteration for columns that are empty in the SQL table (except for SQlite where alteration is limited)
3. <i>(optional)</i> Creates the table if it is missing
4. <i>(optional)</i> Creates missing schemas in Postgres (and potentially other databases that have a schema system)
5. JSON is supported (with pd.to_sql it does not work) with some exceptions (see [Gotchas and caveats](#Gotchas-and-caveats))
6. Fast (except for SQlite where some help is needed)
7. Will work even if not all columns defined in the SQL table are there
8. SQL injection safe (schema, table and column names are escaped and values are given as parameters)
9. _New in version 4.1_: **asynchronous support**. Tested using `aiosqlite` for SQlite, `asyncpg` for PostgreSQL and `aiomysql` for MySQL

# Requirements

* SQlite >= 3.24.4
* Python >= 3.6.4
* See also ./pangres/requirements.txt

## Requirements for sqlalchemy>=2.0

For using `pangres` together with **`sqlalchemy>=2.0`** (sqlalchemy is one of pangres dependencies
listed in requirements.txt) - you will need the following base requirements:
* `alembic>=1.7.2`
* `pandas>=1.4.0`
* Python >= 3.8 (`pandas>=1.4.0` only supports Python >=3.8)

## Requirements for asynchronous engines

For using asynchronous engines (such as `aiosqlite`, `asyncpg` or `aiomysql`) you will need **Python >= 3.8**.

# Gotchas and caveats

## All flavors
1. We can't create JSON columns automatically, but we can insert JSON like objects (list, dict) in existing JSON columns.

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

Ideally such features will be integrated into pandas since there is already a [PR on the way](https://github.com/pandas-dev/pandas/pull/29636) and I would like to give the option to add columns via another PR.

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

* Postgres (asynchronous)
```
pip install asyncpg
```

* MySQL (asynchronous)
```
pip install aiomysql
```

* SQLite (asynchronous)
```
pip install aiosqlite
```

# Usage

Head over to [pangres' wiki](https://github.com/ThibTrip/pangres/wiki)! Note that the wiki is also available
locally under the [wiki folder](https://github.com/ThibTrip/pangres/tree/master/wiki).

Note:

The wiki is generated with a command which uses my library [npdoc_to_md](https://github.com/ThibTrip/npdoc_to_md).
It must be installed with `pip install npdoc_to_md` and you will also need the extra dependency `fire` which you
can install with `pip install fire`. Replace `$DESTINATION_FOLDER` with the folder of you choice in the command below:

```bash
npdoc-to-md render-folder ./wiki_templates $DESTINATION_FOLDER
```

# Contributing

Pull requests/issues are welcome.

# Development

I develop the library inside of **Jupyter Lab** using the [**jupytext**](https://github.com/mwouts/jupytext) extension.

I recommend using this extension for the best experience.
It will split code blocks within modules in notebook cells and will allow **interactive development**.

If you wish you can also use the provided **conda environment** (see `environment.yml` file) inside of Jupyter Lab/Notebook
thanks to [**nb_conda_kernels**](https://github.com/Anaconda-Platform/nb_conda_kernels).

# Testing

## Pytest

You can test one or multiple of the following SQL flavors (you will of course need a live database for this): PostgreSQL, SQlite or MySQL.

NOTE: in one of the tests of `pangres` we will try to drop and then create a PostgreSQL schema called `pangres_create_schema_test`. If the schema existed and was not empty an error will be raised.

Clone pangres then set your curent working directory to the root of the cloned repository folder. Then use the commands below. You will have to replace the following variables in those commands:
* `SQLITE_CONNECTION_STRING`: replace with a SQlite sqlalchemy connection string (e.g. "sqlite:///test.db")
* `ASYNC_SQLITE_CONNECTION_STRING`: replace with an asynchronous SQlite sqlalchemy connection string (e.g. "sqlite+aiosqlite:///test.db")
* `POSTGRES_CONNECTION_STRING`: replace with a Postgres sqlalchemy connection string (e.g. "postgres:///user:password@localhost:5432/database"). Specifying schema is optional for postgres (will default to public)
* `ASYNC_POSTGRES_CONNECTION_STRING`: replace with an asynchronous Postgres sqlalchemy connection string (e.g. "postgres+asyncpg:///user:password@localhost:5432/database"). Specifying schema is optional for postgres (will default to public)
* `MYSQL_CONNECTION_STRING`: replace with a MySQL sqlalchemy connection string (e.g. "mysql+pymysql:///user:password@localhost:3306/database")
* `ASYNC_MYSQL_CONNECTION_STRING`: replace with an asynchronous MySQL sqlalchemy connection string (e.g. "mysql+aiomysql:///user:password@localhost:3306/database")
* `PG_SCHEMA` (optional): schema for postgres (defaults to public)

```bash
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
pytest -s -v pangres --cov=pangres --doctest-modules --async_sqlite_conn=$ASYNC_SQLITE_CONNECTION_STRING --sqlite_conn=$SQLITE_CONNECTION_STRING --async_pg_conn=$ASYNC_POSTGRES_CONNECTION_STRING --pg_conn=$POSTGRES_CONNECTION_STRING --async_mysql_conn=$ASYNC_MYSQL_CONNECTION_STRING --mysql_conn=$MYSQL_CONNECTION_STRING --pg_schema=tests --benchmark-group-by=func,param:engine,param:nb_rows --benchmark-columns=min,max,mean,rounds --benchmark-sort=name --benchmark-name=short
```

Additionally, the following flags could be of interest for you:
* `-x` for stopping at the first failure
* `--benchmark-only` for only testing benchmarks
* `--benchmark-skip` for skipping benchmarks

## flake8

flake8 must run without errors for pipelines to succeed.
If you are not using the conda environment, you can install flake8 with: `pip install flake8`.

To test flake8 locally you can simply execute this command:

```
flake8 .
```

