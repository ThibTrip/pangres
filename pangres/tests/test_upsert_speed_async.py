#!/usr/bin/env python
# coding: utf-8
"""
Async variant of module test_upsert. See its docstring.

Pandas does not support async so we won't do tests for creating tables.

Also pytest-benchmark does not support async which is why we turn our
coroutines `adrop_table_if_exists` and `aupsert` to sync (we have
to for async engines because their already implemented sync variants
would not work)
"""
import asyncio
import json
from math import floor
from sqlalchemy import VARCHAR
from pangres import aupsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import adrop_table_if_exists


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

async def acreate_or_update(engine, schema, if_row_exists):
    # MySQL does not want flexible text length in indices/PK
    dtype={'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    await aupsert(engine=engine, df=df, schema=schema,
                  table_name=pangres_table_name, if_row_exists=if_row_exists,
                  dtype=dtype)

# we need those two functions for async engines
def create_or_update_sync(engine, schema, if_row_exists):
    loop = asyncio.get_event_loop()
    coroutines = [acreate_or_update(engine=engine, schema=schema, if_row_exists=if_row_exists)]
    tasks = asyncio.gather(*coroutines, return_exceptions=True)
    results = loop.run_until_complete(tasks)
    for r in results:
        if isinstance(r, Exception):
            raise r

def drop_table_if_exists_sync(engine, schema, table_name):
    loop = asyncio.get_event_loop()
    coroutines = [adrop_table_if_exists(engine=engine, schema=schema, table_name=table_name)]
    tasks = asyncio.gather(*coroutines, return_exceptions=True)
    results = loop.run_until_complete(tasks)
    for r in results:
        if isinstance(r, Exception):
            raise r
# -

# # Creation speed


# do all tests only once because:
#
# 1. we have enough data as is
# 2. we would have to drop and recreate table at each loop...

def test_creation_speed_with_upsert(engine, schema, benchmark):
    # benchmark is implicitly imported with pytest
    drop_table_if_exists_sync(engine=engine, schema=schema, table_name=pangres_table_name)
    f = lambda: create_or_update_sync(engine=engine, schema=schema, if_row_exists='update')
    benchmark.pedantic(f, rounds=1, iterations=1)


# # Upsert overwrite speed

def test_upsert_overwrite_speed_with_upsert(engine, schema, benchmark):
    f = lambda: create_or_update_sync(engine=engine, schema=schema, if_row_exists='update')
    benchmark.pedantic(f, rounds=1, iterations=1)


# # Upsert keep speed

def test_upsert_keep_speed_with_upsert(engine, schema, benchmark):
    f = lambda: create_or_update_sync(engine=engine, schema=schema, if_row_exists='ignore')
    benchmark.pedantic(f, rounds=1, iterations=1)
