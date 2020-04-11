#!/usr/bin/env python
# coding: utf-8
import json
from math import floor
from sqlalchemy import VARCHAR
from pangres import upsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import drop_table_if_exists


# # Test data


# +
df = _TestsExampleTable.create_example_df(nb_rows=20000)

# pandas can't handle JSON
# so for testing the speed of pd.to_sql we need to cast list and dicts to str
json_like_to_str = lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
df_no_json_like = df.assign(favorite_colors=lambda df: df['favorite_colors'].map(json_like_to_str))


# -


# # Helpers

# +
# table for speed test with pangres
pangres_table_name = 'test_speed_pangres'
# table for speed test with pd.to_sql
pandas_table_name = 'test_speed_pandas_create_table'

def create_or_update(engine, schema, if_row_exists):
    # MySQL does not want flexible text length in indices/PK
    dtype={'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    upsert(engine=engine, df=df, schema=schema,
           table_name=pangres_table_name, if_row_exists=if_row_exists,
           dtype=dtype)

def pd_to_sql(engine, schema):
    # MySQL does not want flexible text length in indices/PK
    dtype={'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    # we need this for SQlite when using pandas table creation
    # since we cannot use more than 999 parameters in a parameterized query
    if 'sqlite' in engine.dialect.dialect_description:
        col_len = len(df.columns) + len(df.index.names)
        chunksize = floor(999 / col_len)
    else:
        chunksize=None
    
    df_no_json_like.to_sql(con=engine, schema=schema,
                           name=pandas_table_name,
                           method='multi', chunksize=chunksize,
                           dtype=dtype)
# -

# # Creation speed


# do all tests only once because:
#
# 1. we have enough data as is
# 2. we would have to drop and recreate table at each loop...

def test_creation_speed_with_upsert(engine, schema, benchmark):
    # benchmark is implicitly imported with pytest
    drop_table_if_exists(engine=engine, schema=schema, table_name=pangres_table_name)
    benchmark.pedantic(lambda: create_or_update(engine=engine, schema=schema, if_row_exists='update'),
                       rounds=1, iterations=1)


# # Upsert overwrite speed

def test_upsert_overwrite_speed_with_upsert(engine, schema, benchmark):
    benchmark.pedantic(lambda: create_or_update(engine=engine, schema=schema, if_row_exists='update'),
                       rounds=1, iterations=1)


# # Upsert keep speed

def test_upsert_keep_speed_with_upsert(engine, schema, benchmark):
    benchmark.pedantic(lambda: create_or_update(engine=engine, schema=schema, if_row_exists='ignore'),
                       rounds=1, iterations=1)


# # Compare with pandas

def test_creation_speed_with_pandas(engine, schema, benchmark):
    drop_table_if_exists(engine=engine, schema=schema, table_name=pandas_table_name)
    benchmark.pedantic(lambda: pd_to_sql(engine=engine, schema=schema),
                       rounds=1, iterations=1)
