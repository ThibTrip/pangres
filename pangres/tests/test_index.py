#!/usr/bin/env python
# coding: utf-8
"""
Here we test different indices:
1. MultiIndex
2. Indices with NULL

# Note
Other problems with indices such as duplicated values are handled by pangres.helpersPandasSpecialEngine.
"""
import pandas as pd
from sqlalchemy import VARCHAR
from sqlalchemy.exc import IntegrityError
from pangres import upsert
from pangres.tests.conftest import drop_table_if_exists


# # Config

# +
# for creating table with MultiIndex
index_col = ['ix1', 'ix2', 'ix3']
df_multiindex = pd.DataFrame({'ix1': [1, 1], 'ix2': ['test', 'test2'], 
                              'ix3': [pd.Timestamp('2019-01-01'), pd.Timestamp('2019-01-02')],
                              'foo': [1, 2]}).set_index(index_col)

# for inserting values
df_multiindex2 = pd.DataFrame({'ix1': [2, 2], 'ix2': ['test', 'test2'], 
                              'ix3': [pd.Timestamp('2019-01-01'), pd.Timestamp('2019-01-02')],
                              'foo': [1, 2]}).set_index(index_col)

default_args = {'if_row_exists':'update',
                'adapt_dtype_of_empty_db_columns':False}


# -

# # Tests

# ## Test MultiIndex

def test_create_and_insert_table_multiindex(engine, schema):
    table_name = 'test_multiindex'
    namespace = f'{schema}.{table_name}' if schema is not None else table_name
    drop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'ix2':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None

    # create
    upsert(engine=engine, schema=schema, df=df_multiindex, table_name=table_name, dtype=dtype, **default_args)
    df_db = pd.read_sql(f'SELECT * FROM {namespace}', con=engine, index_col=index_col)

    # insert
    upsert(engine=engine, schema=schema, df=df_multiindex2, table_name=table_name, dtype=dtype, **default_args)
    df_db = pd.read_sql(f'SELECT * FROM {namespace}', con=engine, index_col=index_col)


# ## Test index with null value

def test_index_with_null(engine, schema):
    df = pd.DataFrame({'ix':[None], 'foo': [2]}).set_index('ix')
    table_name='test_index_with_null'
    drop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    # don't test for mysql since only a warning is raised and the line is skipped
    if not 'mysql' in engine.dialect.dialect_description:
        try:
            upsert(engine=engine, schema=schema, df=df, table_name=table_name, **default_args)
            raise ValueError('upsert did not fail as expected with null value in index')
        except IntegrityError as e:
            print(f'upsert failed as expected with null value in index. Error was:\n\n{e}')
