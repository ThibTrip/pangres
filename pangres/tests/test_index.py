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
import pytest
from sqlalchemy import VARCHAR, text
from sqlalchemy.exc import IntegrityError
from pangres import upsert
from pangres.tests.conftest import AutoDropTableContext


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

default_args = {'if_row_exists':'update'}


# -

# # Tests

# ## Test MultiIndex

def test_create_and_insert_table_multiindex(engine, schema):
    # local helper
    def read_from_db(namespace):
        with engine.connect() as connection:
            df_db = pd.read_sql(text(f'SELECT * FROM {namespace}'), con=connection, index_col=index_col)

    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'ix2':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    with AutoDropTableContext(engine=engine, schema=schema, dtype=dtype, table_name='test_multiindex') as ctx:
        # create
        upsert(engine=engine, schema=schema, df=df_multiindex, table_name=ctx.table_name, dtype=ctx.dtype, **default_args)
        db_db = read_from_db(ctx.namespace)
        # insert
        upsert(engine=engine, schema=schema, df=df_multiindex2, table_name=ctx.table_name, dtype=ctx.dtype, **default_args)
        db_db = read_from_db(ctx.namespace)


# ## Test index with null value

def test_index_with_null(engine, schema):
    df = pd.DataFrame({'ix':[None, 0], 'test': [0, 1]}).set_index('ix')
    with AutoDropTableContext(engine=engine, schema=schema, table_name='test_index_with_null') as ctx:
        with pytest.raises(IntegrityError) as excinfo:
            upsert(engine=engine, schema=schema, df=df, table_name=ctx.table_name, **default_args)
            # don't test error for mysql since only a warning is raised and the line is skipped
            if 'mysql' in engine.dialect.dialect_description:
                pytest.skip()


# ## Test only index

@pytest.mark.parametrize("if_row_exists", ['update', 'ignore'])
def test_only_index(engine, schema, if_row_exists):
    # upsert df with only index
    df = pd.DataFrame({'ix':[1]}).set_index('ix')
    with AutoDropTableContext(engine=engine, schema=schema, table_name='test_index_only') as ctx:
        upsert(engine=engine, schema=schema, df=df, table_name=ctx.table_name, if_row_exists=if_row_exists)
        # check data integrity
        with engine.connect() as connection:
            df_db = pd.read_sql(text(f'SELECT * FROM {ctx.namespace}'), con=connection)
        assert 'ix' in df_db.columns
        assert len(df_db) > 0
        assert df_db['ix'].iloc[0] == 1
