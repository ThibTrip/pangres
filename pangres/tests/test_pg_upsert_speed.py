#!/usr/bin/env python
# coding: utf-8



import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine

from pangres import pg_upsert
from pangres.tests.conftest import TestDB


# # Config



testdb = TestDB()


# # Test data


df = pd.DataFrame({
    'empty_col': [None, None],
    'int': [1, 2],
    'float': [1.2, 2.3],
    'nan': [np.nan, np.nan],
    'ts_tz_special': [
        pd.Timestamp('2019-12-01', tz='US/Pacific'),
        pd.Timestamp('2019-08-01', tz='US/Pacific')
    ],
    'text': ['foo', 'bar'],
    'tuples': [('foo', 'bar'), ('bar', 'foo')]
})

df = (pd.concat([df] * 10000, ignore_index=True,
                axis=0).rename_axis(['profileid'], axis='index', inplace=False))


# # Creation speed


# do all tests only once because 1) we have enough data as is
# and 2) we would have to drop and recreate table at each loop...

def test_creation_speed_with_pg_upsert(benchmark):

    def do_upsert():
        pg_upsert(engine=testdb.engine,
                  df=df,
                  schema=testdb.schema,
                  table_name='test_speed_pangres_create_table',
                  if_exists='upsert_overwrite')

    # benchmark is implicitly imported with pytest
    benchmark.pedantic(do_upsert, rounds=1, iterations=1)


# # Upsert overwrite speed



def test_upsert_overwrite_speed_with_pg_upsert(benchmark):

    def do_upsert():
        pg_upsert(engine=testdb.engine,
                  df=df,
                  schema=testdb.schema,
                  table_name='test_speed_pangres_upsert_overwrite',
                  if_exists='upsert_overwrite')

    benchmark.pedantic(do_upsert, rounds=1, iterations=1)


# # Upsert keep speed



def test_upsert_keep_speed_with_pg_upsert(benchmark):

    def do_upsert():
        pg_upsert(engine=testdb.engine,
                  df=df,
                  schema=testdb.schema,
                  table_name='test_speed_pangres_upsert_keep',
                  if_exists='upsert_keep')

    benchmark.pedantic(do_upsert, rounds=1, iterations=1)


# # Compare with pandas



def test_creation_speed_with_pandas(benchmark):

    def do_upsert():
        df.to_sql(con=testdb.engine,
                  schema=testdb.schema,
                  name='test_speed_pandas_create_table',
                  method='multi')

    benchmark.pedantic(do_upsert, rounds=1, iterations=1)