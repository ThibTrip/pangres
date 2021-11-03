#!/usr/bin/env python
# coding: utf-8
"""
Async variant of module test_upsert. See its docstring.
"""
import pandas as pd
import pytest
import random
from sqlalchemy import VARCHAR
from pangres import aupsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import ReaderSQLExampleTables, adrop_table_if_exists


# # Config

table_name = 'test_upsert'
# there is a difference with the sync tests here: adding columns and changing their types
# is not implemented yet
default_args = {'table_name':table_name, 'create_schema':True}


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

@pytest.mark.asyncio
async def test_create_table(engine, schema):
    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    
    await adrop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    await aupsert(engine=engine, schema=schema, df=df, if_row_exists='update', dtype=dtype, **default_args)
    df_db = await ReaderSQLExampleTables.aread(engine=engine, schema=schema, table_name=table_name)
    pd.testing.assert_frame_equal(df, df_db)

# ## INSERT UPDATE 

@pytest.mark.asyncio
async def test_upsert_update(engine, schema):
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None

    await aupsert(engine=engine, schema=schema, df=df2, if_row_exists='update', dtype=dtype, **default_args)
    df_db = await ReaderSQLExampleTables.aread(engine=engine, schema=schema, table_name=table_name)
    pd.testing.assert_frame_equal(df2, df_db)

# ## INSERT IGNORE

@pytest.mark.asyncio
async def test_upsert_ignore(engine, schema):
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None

    await adrop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    for _df in (df, df3):
        await aupsert(engine=engine, schema=schema, df=_df, if_row_exists='ignore', dtype=dtype, **default_args)
    df_db = await ReaderSQLExampleTables.aread(engine=engine, schema=schema, table_name=table_name)
    expected = pd.concat((df, df3.tail(1)), axis=0)
    pd.testing.assert_frame_equal(expected, df_db)

# # ~~Add colums with crappy names and~~ insert crappy text values
#
# There is a difference with the sync test here: we don't do test for adding columns as this is not supported yet.
#
# We will insert our bad texts inside of the column "email" instead of in a new column called "text".

@pytest.mark.asyncio
async def test_crappy_text_insert(engine, schema):
    is_mysql = 'mysql' in engine.dialect.dialect_description
    dtype = {'profileid':VARCHAR(10)} if is_mysql else None

    # mix crappy letters with a few normal ones
    crap_char_seq = """/_- ?§$&"',:;*()%[]{}|<>=!+#""" + "\\" + "sknalji"  

    # add crappy text in the column 'email'
    create_random_text = lambda: ''.join([random.choice(crap_char_seq) for i in range(10)])

    df_test = (pd.DataFrame({'email': [create_random_text() for i in range(10)]})
               .rename_axis(['profileid'], axis='index', inplace=False))
    await aupsert(engine=engine, schema=schema, df=df_test, if_row_exists='update', dtype=dtype, **default_args)

# # Another test with the column name `values` (see issue #34 of pangres)

@pytest.mark.asyncio
async def test_column_named_values(engine, schema):
    df = pd.DataFrame({'values': range(5, 9)}, index=pd.Index(range(1, 5), name='idx'))
    await aupsert(engine=engine, schema=schema, df=df, if_row_exists='update', table_name='test_column_values')
