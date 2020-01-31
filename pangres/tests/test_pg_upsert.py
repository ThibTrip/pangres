#!/usr/bin/env python
# coding: utf-8



from sqlalchemy import create_engine
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
table_name = 'test_upsert'



# # Test data



df = pd.DataFrame({
    'empty_col': [None, None],
    'int': [1, 2],
    'float': [1.2, 2.3],
    'ts_tz': [
        pd.Timestamp('2019-12-01', tz='UTC'),
        pd.Timestamp('2019-08-01', tz='UTC')
    ],
    'text': ['foo', 'bar']
}).rename_axis(['profileid'], axis='index', inplace=False)

df_2 = df.iloc[[0, 0], :].copy(deep=True)
df_2.index = [1, 2]
df_2.index.name = 'profileid'

df_3 = pd.DataFrame({
    'empty_col': [1, None, 2],
    'int': [2, None, 3],
    'float': [1.2, None, 2.1],
    'ts_tz': [
        pd.Timestamp('2011-12-01', tz='UTC'), pd.NaT,
        pd.Timestamp('2012-08-01', tz='UTC')
    ],
    'text': ['banana', None, 'tomato']
}).rename_axis(['profileid'], axis='index', inplace=False)


# # Tests
# ORDER MATTERS!



def test_create_table():
    pg_upsert(engine=testdb.engine,
              df=df,
              schema=testdb.schema,
              table_name=table_name,
              if_exists='upsert_overwrite',
              create_schema=True,
              add_new_columns=True,
              adapt_dtype_of_empty_db_columns=True)

    df_db = pd.read_sql(f'SELECT * FROM {testdb.schema}.{table_name}',
                        con=testdb.engine,
                        index_col='profileid')
    df_db['ts_tz'] = pd.to_datetime(df_db['ts_tz'], utc=True)
    pd.testing.assert_frame_equal(df, df_db)


def test_upsert_keep():

    pg_upsert(engine=testdb.engine,
              df=df_2,
              schema=testdb.schema,
              table_name=table_name,
              if_exists='upsert_keep',
              create_schema=True,
              add_new_columns=True,
              adapt_dtype_of_empty_db_columns=True)

    df_db = pd.read_sql(f'SELECT * FROM {testdb.schema}.{table_name}',
                        con=testdb.engine,
                        index_col='profileid')

    # row 0 should be the same as row 2 (this is how we defined df_2)
    pd.testing.assert_series_equal(df_db.iloc[0],
                                   df_db.iloc[2],
                                   check_names=False)

    # also row 1 should have been kept identical
    pd.testing.assert_series_equal(df.iloc[1], df_db.iloc[1], check_names=False)


def test_upsert_overwrite():
    pg_upsert(engine=testdb.engine,
              df=df_3,
              schema=testdb.schema,
              table_name=table_name,
              if_exists='upsert_overwrite',
              create_schema=True,
              add_new_columns=True,
              adapt_dtype_of_empty_db_columns=True)

    df_db = pd.read_sql(f'SELECT * FROM  {testdb.schema}.{table_name}',
                        con=testdb.engine,
                        index_col='profileid')
    
    df_db['ts_tz'] = pd.to_datetime(df_db['ts_tz'], utc = True)

    expected = pd.DataFrame(
        data=[[
            1.0, 2.0, 1.2,
            datetime.datetime(2011,
                              12,
                              1,
                              0,
                              0,
                              0,
                              tzinfo=datetime.timezone.utc), 'banana'
        ], [np.nan, np.nan, np.nan, None, None],
              [
                  2.0, 3.0, 2.1,
                  datetime.datetime(2012,
                                    8,
                                    1,
                                    0,
                                    0,
                                    0,
                                    tzinfo=datetime.timezone.utc), 'tomato'
              ]],
        columns=df_db.columns,
        index=df_db.index).rename_axis(['profileid'],
                                       axis='index',
                                       inplace=False)

    pd.testing.assert_frame_equal(df_db, expected)


def test_try_to_break_pg_upsert():
    
    # mix crappy letters with a few normal ones
    crap_char_seq = """/_- ?§$&"',:;*()%[]{}|<>=!+#""" + "\\" + "sknalji"  
    # NOTE: those characters may cause failures due to psycopg2: ["%", "(", ")"]
    # but they are handled in pandas_pg_upsert.helper.PandasSpecialEngine
    # when the argument clean_column_names is True

    # add crappy column names
    for i in range(5):
        random_crappy_col_name = ''.join(
            [random.choice(crap_char_seq) for i in range(50)])

        df_test = (pd.DataFrame({
            random_crappy_col_name: ['test', None]
        }).rename_axis(['profileid'], axis='index', inplace=False))

        pg_upsert(engine=testdb.engine,
                  df=df_test,
                  schema=testdb.schema,
                  table_name=table_name,
                  if_exists='upsert_overwrite',
                  add_new_columns=True,
                  adapt_dtype_of_empty_db_columns=True,
                  clean_column_names=True)

    # add crappy text in text columns
    create_random_text = lambda: [
        random.choice(crap_char_seq) for i in range(10)
    ]

    df_test = (pd.DataFrame({
        'text': [create_random_text() for i in range(10)]
    }).rename_axis(['profileid'], axis='index', inplace=False))

    pg_upsert(engine=testdb.engine,
              df=df_test,
              schema=testdb.schema,
              table_name=table_name,
              if_exists='upsert_overwrite',
              add_new_columns=True,
              adapt_dtype_of_empty_db_columns=True)

