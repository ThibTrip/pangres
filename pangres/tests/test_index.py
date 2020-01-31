#!/usr/bin/env python
# coding: utf-8



from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import pandas as pd
import numpy as np
import datetime
import psycopg2
import random
import os

from pangres import pg_upsert
from pangres.tests.conftest import TestDB


# # Config


testdb = TestDB()


# # Tests



def test_create_and_insert_table_multiindex():
    # create
    df = pd.DataFrame({
    'ix1': [1, 1],
    'ix2': ['test', 'test2'],
    'ix3': [pd.Timestamp('2019-01-01'),
            pd.Timestamp('2019-01-02')],
    'foo': [1, 2]
    }).set_index(['ix1', 'ix2', 'ix3'])

    pg_upsert(engine=testdb.engine,
              df=df,
              schema=testdb.schema,
              table_name='test_multiindex',
              if_exists='upsert_overwrite',
              create_schema=True,
              add_new_columns=True,
              adapt_dtype_of_empty_db_columns=True)

    df_db = pd.read_sql(f'SELECT * FROM {testdb.schema}.test_multiindex',
                        con=testdb.engine,
                        index_col=['ix1', 'ix2', 'ix3'])
    pd.testing.assert_frame_equal(df, df_db)

    # insert
    df2 = pd.DataFrame({
        'ix1': [2, 2],
        'ix2': ['test', 'test2'],
        'ix3': [pd.Timestamp('2019-01-01'),
                pd.Timestamp('2019-01-02')],
        'foo': [1, 2]
    }).set_index(['ix1', 'ix2', 'ix3'])

    pg_upsert(engine=testdb.engine,
              df=df2,
              schema=testdb.schema,
              table_name='test_multiindex',
              if_exists='upsert_overwrite',
              create_schema=True,
              add_new_columns=True,
              adapt_dtype_of_empty_db_columns=True)

    df_db = pd.read_sql(f'SELECT * FROM {testdb.schema}.test_multiindex',
                        con=testdb.engine,
                        index_col=['ix1', 'ix2', 'ix3'])
    pd.testing.assert_frame_equal(pd.concat([df, df2]), df_db)


def test_index_with_null():
    df3 = pd.DataFrame({
        'ix1': [None, None],
        'ix2': ['test', 'test2'],
        'ix3': [pd.Timestamp('2019-01-01'),
                pd.Timestamp('2019-01-02')],
        'foo': [1, 2]
    }).set_index(['ix1', 'ix2', 'ix3'])

    try:
        pg_upsert(engine=testdb.engine,
                  df=df3,
                  schema=testdb.schema,
                  table_name='test_index_with_null',
                  if_exists='upsert_overwrite',
                  create_schema=True,
                  add_new_columns=True,
                  adapt_dtype_of_empty_db_columns=True)
    except IntegrityError as e:
        print((f'Insert of a DataFrame with null '
               f'values in index failed as expected. Error was:\n\n{e}'))

def test_non_unique_index():
    df_non_unique_ix = (pd.DataFrame({'profileid':[1,1],
                                      'date':['2018-01-01','2018-01-01'],
                                      'favorite_fruit':['banana','apple']})
                        .set_index(['profileid','date']))
    try:
        pg_upsert(engine=testdb.engine,
                  df=df_non_unique_ix,
                  schema=testdb.schema,
                  table_name='fail_duplicated_index',
                  if_exists='upsert_overwrite',
                  create_schema=True,
                  add_new_columns=True,
                  adapt_dtype_of_empty_db_columns=True)
    except IndexError as e:
        print((f'Insert of a DataFrame with non unique index '
               f'values in index failed as expected. Error was:\n\n{e}'))
