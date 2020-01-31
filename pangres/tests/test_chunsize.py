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



# # Tests



def test_insert_one():
    df = pd.DataFrame({
        'test': [1] * 25
    }).rename_axis(['profileid'], axis='index', inplace=False)

    pg_upsert(engine=testdb.engine,
              df=df,
              schema=testdb.schema,
              table_name='test_insert_chunksize_one',
              if_exists='upsert_overwrite',
              chunksize=1)

    df_db = pd.read_sql(f'SELECT * FROM {testdb.schema}.test_insert_chunksize_one',
                        con=testdb.engine,
                        index_col='profileid')
    pd.testing.assert_frame_equal(df, df_db)


def test_insert_odd_chunksize():
    df = pd.DataFrame({
        'test': [1] * 25
    }).rename_axis(['profileid'], axis='index', inplace=False)

    pg_upsert(engine=testdb.engine,
              df=df,
              schema=testdb.schema,
              table_name='test_insert_chunksize_odd',
              if_exists='upsert_overwrite',
              chunksize=3)

    df_db = pd.read_sql(f'SELECT * FROM {testdb.schema}.test_insert_chunksize_odd',
                        con=testdb.engine,
                        index_col='profileid')
    pd.testing.assert_frame_equal(df, df_db)

