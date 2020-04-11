#!/usr/bin/env python
# coding: utf-8
"""
Here we want to test if:
1. the IGNORE/UPDATE logic works properly
2. NULL values do not cause issues
3. Crappy text insert and column names does not cause issues
"""
import pandas as pd
import random
from sqlalchemy import VARCHAR
from pangres import upsert, fix_psycopg2_bad_cols
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import read_example_table_from_db, drop_table_if_exists


# # Config

table_name = 'test_upsert'
default_args = {'table_name':table_name,
                'create_schema':True,
                'add_new_columns':True,
                'adapt_dtype_of_empty_db_columns':False}


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

# ## Create table

def test_create_table(engine, schema):
    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    
    drop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    upsert(engine=engine, schema=schema, df=df, if_row_exists='update', dtype=dtype, **default_args)
    df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=table_name)
    pd.testing.assert_frame_equal(df, df_db)


# ## INSERT UPDATE 

def test_upsert_update(engine, schema):
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None

    upsert(engine=engine, schema=schema, df=df2, if_row_exists='update', dtype=dtype, **default_args)
    df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=table_name)
    pd.testing.assert_frame_equal(df2, df_db)


# ## INSERT IGNORE

def test_upsert_ignore(engine, schema):
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    
    drop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    for _df in (df, df3):
        upsert(engine=engine, schema=schema, df=_df, if_row_exists='ignore', dtype=dtype, **default_args)
    df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=table_name)
    expected = pd.concat((df, df3.tail(1)), axis=0)
    pd.testing.assert_frame_equal(expected, df_db)


# # Add colums with crappy names and insert crappy text values

def test_crappy_text_insert(engine, schema):
    is_mysql = 'mysql' in engine.dialect.dialect_description
    dtype = {'profileid':VARCHAR(10)} if is_mysql else None
    
    # mix crappy letters with a few normal ones
    crap_char_seq = """/_- ?§$&"',:;*()%[]{}|<>=!+#""" + "\\" + "sknalji"  

    # add columns with crappy names
    # don't do this for MySQL which has more strict rules for column names 
    if not is_mysql:
        for i in range(5):
            random_crappy_col_name = ''.join([random.choice(crap_char_seq)
                                              for i in range(50)])

            df_test = (pd.DataFrame({random_crappy_col_name: ['test', None]})
                       .rename_axis(['profileid'], axis='index', inplace=False))

            # psycopg2 can't process columns with "%" or "(" or ")"
            df_test = fix_psycopg2_bad_cols(df_test)
            upsert(engine=engine, schema=schema, df=df_test, if_row_exists='update', dtype=dtype, **default_args)

    # add crappy text in a column named 'text'
    create_random_text = lambda: ''.join([random.choice(crap_char_seq)
                                          for i in range(10)])

    df_test = (pd.DataFrame({'text': [create_random_text() for i in range(10)]})
               .rename_axis(['profileid'], axis='index', inplace=False))
    upsert(engine=engine, schema=schema, df=df_test, if_row_exists='update', dtype=dtype, **default_args)
