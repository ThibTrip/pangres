[![CircleCI](https://circleci.com/gh/ThibTrip/pangres.svg?style=svg&circle-token=3e39be6b969ed02b41d259c279da0d9e63751506)](https://circleci.com/gh/ThibTrip/pangres) [![codecov](https://codecov.io/gh/ThibTrip/pangres/branch/master/graph/badge.svg)](https://codecov.io/gh/ThibTrip/pangres)

# pangres
Postgres upsert with pandas DataFrames (<code>ON CONFLICT DO NOTHING</code> or <code>ON CONFLICT DO UPDATE</code>)
with some additional optional features:

1. Create columns in DataFrame to upsert that do not yet exist in the postgres database
2. Alter column data types in postgres for empty columns that do not match the data types of the DataFrame to upsert.

**IMPORTANT**

Characters "(", ")" and "%" may cause issues in column names. The issue
seems to be directly related to [psycopg2](https://pypi.org/project/psycopg2/) (Python library for interacting with PostgreSQL databases).
There is an option in the main function pangres.pg_upsert to remove those characters automatically
(set clean_column_names to True), see [Usage](#Usage).

# Notes

This is a library I was using in production in private with very good results
and decided to publish.

Ideally such features will be integrated into pandas since there is
already a [PR on the way](https://github.com/pandas-dev/pandas/pull/29636))
and I would like to give the option to add columns via another PR.

In the meantime pangres is here and I think the data type alteration for empty
columns is probably not something for pandas.

There is also [pandabase](https://github.com/notsambeck/pandabase) which does almost
the same thing but my implementation is different.

Btw big thanks to pandabase and the sql part of pandas which helped a lot.

# Installation
```
pip install pangres
```

# Usage
The index of the given DataFrame is used as primary key when creating a table using pandas_pg_upsert.
Further details in the docstring of the function pg_upsert.

```python
import pandas as pd
from sqlalchemy import create_engine
from pangres import pg_upsert

# configure schema, table_name and engine
schema = 'tests'
table_name = 'pg_upsert_test'
engine = create_engine('postgresql://user:password@localhost:5432/mydatabase')

# create/get data
df = pd.DataFrame({'profileid':[0,1],
                    'favorite_fruit':['banana','apple']})
df.set_index('profileid', inplace = True)

# create or update table
# if_exists = 'upsert_overwrite' makes a ON CONFLICT DO UPDATE
# if_exists = 'upsert_keep' makes a ON CONFLICT DO NOTHING
# this option does not matter for table creation
pg_upsert(engine=engine,
          df=df,
          schema=schema,
          table_name=table_name,
          if_exists='upsert_overwrite',
          create_schema=True, # default, creates schema if it does not exist
          add_new_columns=True, # default, adds any columns that are not in the postgres table
          adapt_dtype_of_empty_db_columns=True, # converts data type in postgres for empty columns
                                                # (if we finally have data and it is necessary)
          # next option will remove ")", "(" and "%"
          # if those characters are present in the column names
          # as those characters may cause issues with psycopg2
          # if it is False (default) the aforementionned characters will raise an Exception!
          clean_column_names=True,
          chunksize=10000) # default, inserts 10000 rows at once
```


# Contributing

Pull requests/issues are welcome.

# Testing

Clone pangres then set your curent working directory to the root of the cloned repository folder.

Then use these commands:

```
# 1) Create and activate the build environment
conda env create -f environment.yml
conda activate pangres-dev
# 2) Install pangres in editable mode (changes are reflected upon reimporting)
pip install -e .
# 3) Replace sqlalchemy postgreSQL connection string (and schema if necessary) in ./pangres/tests/conftest.py
# More info on connection strings here: https://docs.sqlalchemy.org/en/13/core/engines.html
# **WARNING**: everything in the test schema will be deleted in cascade before tests!
# **WARNING2**: please use a local database with dummy username and password
# or fetch your credentials from os.env or a file so that in case you accidentaly
# push your connection string no confidential  information is leaked!
# 4) Run pytest (--cov=./pangres shows coverage only for pangres)
pytest pangres --cov=./pangres
```
