#!/usr/bin/env python
# coding: utf-8
"""
Async variant of module test_index. See its docstring.
"""
import pandas as pd
import pytest
from sqlalchemy import VARCHAR, text
from sqlalchemy.exc import IntegrityError
from pangres import aupsert
from pangres.tests.conftest import adrop_table_if_exists


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

@pytest.mark.asyncio
async def test_create_and_insert_table_multiindex(engine, schema):
    table_name = 'test_multiindex'
    namespace = f'{schema}.{table_name}' if schema is not None else table_name
    await adrop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'ix2':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None

    # local helper
    async def read_from_db():
        async with engine.connect() as connection:
            proxy = await connection.execute(text(f'SELECT * FROM {namespace}'))
            results = [r._asdict() for r in proxy.all()]
            return pd.DataFrame(results).set_index(index_col)

    # create
    await aupsert(engine=engine, schema=schema, df=df_multiindex, table_name=table_name, dtype=dtype, **default_args)
    db_db = await read_from_db()

    # insert
    await aupsert(engine=engine, schema=schema, df=df_multiindex2, table_name=table_name, dtype=dtype, **default_args)
    db_db = await read_from_db()

# ## Test index with null value

@pytest.mark.asyncio
async def test_index_with_null(engine, schema):
    df = pd.DataFrame({'ix':[None], 'foo': [2]}).set_index('ix')
    table_name='test_index_with_null'
    await adrop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    # don't test for mysql since only a warning is raised and the line is skipped
    if not 'mysql' in engine.dialect.dialect_description:
        try:
            await aupsert(engine=engine, schema=schema, df=df, table_name=table_name, **default_args)
            raise ValueError('upsert did not fail as expected with null value in index')
        except IntegrityError as e:
            print(f'upsert failed as expected with null value in index. Error was:\n\n{e}')

# ## Test only index

# +
# using pytest.mark.asyncio is mandatory
# and combined with pytest parametrize this makes coroutines
# with different parameters execute in parallel
# this is problematic because we are dropping tables
# so we need to make multiple test functions instead
async def execute_test_only_index(engine, schema, if_row_exists):
    # upsert df with only index
    df = pd.DataFrame({'ix':[1]}).set_index('ix')
    table_name='test_index_only'
    await adrop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    await aupsert(engine=engine, schema=schema, df=df, table_name=table_name, if_row_exists=if_row_exists)

    # check data integrity
    namespace = f'{schema}.{table_name}' if schema is not None else table_name
    async with engine.connect() as connection:
        proxy = await connection.execute(text(f'SELECT * FROM {namespace}'))
        results = [r._asdict() for r in proxy.all()]
        df_db = pd.DataFrame(results)

    assert 'ix' in df_db.columns
    assert len(df_db) > 0
    assert df_db['ix'].iloc[0] == 1

@pytest.mark.asyncio
async def test_only_index_on_conflict_ignore(engine, schema):
    await execute_test_only_index(engine=engine, schema=schema, if_row_exists='ignore')

@pytest.mark.asyncio
async def test_only_index_on_conflict_update(engine, schema):
    await execute_test_only_index(engine=engine, schema=schema, if_row_exists='update')
