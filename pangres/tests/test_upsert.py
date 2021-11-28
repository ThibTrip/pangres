#!/usr/bin/env python
# coding: utf-8
"""
Here we want to test if:
1. the IGNORE/UPDATE logic works properly
2. NULL values do not cause issues
3. Crappy text insert and column names does not cause issues
"""
import pandas as pd
from sqlalchemy import VARCHAR
from pangres import upsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import read_example_table_from_db, AutoDropTableContext


# # Config

table_name = 'test_upsert'


# # Test data

# +
df = _TestsExampleTable.create_example_df(nb_rows=5)
# test for NULL values except for boolean column
df.iloc[0,[ix for ix, col in enumerate(df.columns) if col != 'likes_pizza']] = None

# test for update
df2 = _TestsExampleTable.create_example_df(nb_rows=6)

# test for ignore
df3 = _TestsExampleTable.create_example_df(nb_rows=6)


# -

# # Tests
# ORDER MATTERS!

# ## 1. Create table

def test_create_table(engine, schema):
    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    # IMPORTANT! don't drop one exit of context manager, we need the table for the next tests
    with AutoDropTableContext(engine=engine, schema=schema, table_name=table_name, df=df, drop_on_exit=False) as ctx:
        upsert(engine=engine, schema=schema, df=df, if_row_exists='update', dtype=dtype, table_name=table_name)
        df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=table_name)
        pd.testing.assert_frame_equal(df, df_db)


# ## 2. INSERT UPDATE 

# continues previous test!
def test_upsert_update(engine, schema):
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    upsert(engine=engine, schema=schema, df=df2, if_row_exists='update', dtype=dtype, table_name=table_name)
    df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=table_name)
    pd.testing.assert_frame_equal(df2, df_db)


# ## 3. INSERT IGNORE

def test_upsert_ignore(engine, schema):
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    with AutoDropTableContext(engine=engine, schema=schema, table_name=table_name) as ctx:
        for _df in (df, df3):
            upsert(engine=engine, schema=schema, df=_df, if_row_exists='ignore', dtype=dtype, table_name=table_name)
        df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=table_name)
        expected = pd.concat((df, df3.tail(1)), axis=0)
        pd.testing.assert_frame_equal(expected, df_db)
