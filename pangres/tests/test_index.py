#!/usr/bin/env python
# coding: utf-8
# +
"""
Here we test different indices:
1. MultiIndex
2. Indices with NULL

# Note
Other problems with indices such as duplicated values are handled by pangres.helpersPandasSpecialEngine.
"""
import pandas as pd
import pytest
from sqlalchemy import VARCHAR
from sqlalchemy.exc import IntegrityError

# local imports
from pangres import aupsert, upsert
from pangres.tests.conftest import (aselect_table, adrop_table_between_tests,
                                    drop_table_between_tests,
                                    select_table, sync_or_async_test, TableNames)
# -


# # Config

# +
# for creating table with MultiIndex
index_col = ['ix1', 'ix2', 'ix3']
df_multiindex = pd.DataFrame({'ix1': [1, 1],
                              'ix2': ['test', 'test2'],
                              'ix3': [pd.Timestamp('2019-01-01'), pd.Timestamp('2019-01-02')],
                              'foo': [1, 2]}).set_index(index_col)

# for inserting values
df_multiindex2 = pd.DataFrame({'ix1': [2, 2],
                               'ix2': ['test', 'test2'],
                               'ix3': [pd.Timestamp('2019-01-01'), pd.Timestamp('2019-01-02')],
                               'foo': [1, 2]}).set_index(index_col)


# -

# # Sync and async variants for tests
#
# (`run_test_foo`|`run_test_foo_async`) -> `test_foo`

# ## Test MultiIndex

# +
@drop_table_between_tests(table_name=TableNames.MULTIINDEX)
def run_test_create_and_insert_table_multiindex(engine, schema):
    # local helpers
    table_name = TableNames.MULTIINDEX

    def read_from_db():
        df_db = select_table(engine=engine, schema=schema, table_name=table_name)
        df_db['ix3'] = pd.to_datetime(df_db['ix3'])
        return df_db.set_index(index_col)

    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'ix2': VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None

    # create
    upsert(con=engine, schema=schema, df=df_multiindex, table_name=table_name, dtype=dtype, if_row_exists='update')
    df_db = read_from_db()
    pd.testing.assert_frame_equal(df_db, df_multiindex)

    # insert
    upsert(con=engine, schema=schema, df=df_multiindex2, table_name=table_name, dtype=dtype, if_row_exists='update')
    df_db = read_from_db()
    pd.testing.assert_frame_equal(df_db, pd.concat(objs=[df_multiindex, df_multiindex2]))


@adrop_table_between_tests(table_name=TableNames.MULTIINDEX)
async def run_test_create_and_insert_table_multiindex_async(engine, schema):
    # local helpers
    table_name = TableNames.MULTIINDEX

    async def read_from_db():
        df_db = await aselect_table(engine=engine, schema=schema, table_name=table_name)
        df_db['ix3'] = pd.to_datetime(df_db['ix3'])
        return df_db.set_index(index_col)

    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'ix2': VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None

    # create
    await aupsert(con=engine, schema=schema, df=df_multiindex, table_name=table_name,
                  dtype=dtype, if_row_exists='update')
    df_db = await read_from_db()
    pd.testing.assert_frame_equal(df_db, df_multiindex)

    # insert
    await aupsert(con=engine, schema=schema, df=df_multiindex2, table_name=table_name,
                  dtype=dtype, if_row_exists='update')
    df_db = await read_from_db()
    pd.testing.assert_frame_equal(df_db, pd.concat(objs=[df_multiindex, df_multiindex2]))


# -

# ## Test index with null value

# +
@drop_table_between_tests(table_name=TableNames.INDEX_WITH_NULL)
def run_test_index_with_null(engine, schema):
    df = pd.DataFrame({'ix': [None, 0], 'test': [0, 1]}).set_index('ix')
    with pytest.raises(IntegrityError):
        upsert(con=engine, schema=schema, df=df, table_name=TableNames.INDEX_WITH_NULL, if_row_exists='update')
        # don't test error for mysql since only a warning is raised and the line is skipped
        if 'mysql' in engine.dialect.dialect_description:  # pragma: no cover
            pytest.skip('not tested with mysql as only a warning is issued and the line is skipped. '
                        "Perhaps we could capture the warning with pytest?")


@adrop_table_between_tests(table_name=TableNames.INDEX_WITH_NULL)
async def run_test_index_with_null_async(engine, schema):
    df = pd.DataFrame({'ix': [None, 0], 'test': [0, 1]}).set_index('ix')
    with pytest.raises(IntegrityError):
        await aupsert(con=engine, schema=schema, df=df, table_name=TableNames.INDEX_WITH_NULL, if_row_exists='update')
        # don't test error for mysql since only a warning is raised and the line is skipped
        if 'mysql' in engine.dialect.dialect_description:  # pragma: no cover
            pytest.skip('not tested with mysql as only a warning is issued and the line is skipped. '
                        "Perhaps we could capture the warning with pytest?")


# -

# ## Test only index

# +
@drop_table_between_tests(table_name=TableNames.INDEX_ONLY_INSERT)
def run_test_only_index(engine, schema, if_row_exists):
    # config
    table_name = TableNames.INDEX_ONLY_INSERT

    # upsert df with only index
    df = pd.DataFrame({'ix': [1]}).set_index('ix')
    upsert(con=engine, schema=schema, df=df, table_name=table_name, if_row_exists=if_row_exists)

    # check data integrity
    df_db = select_table(engine=engine, schema=schema, table_name=table_name)
    assert 'ix' in df_db.columns
    assert len(df_db) > 0
    assert df_db['ix'].iloc[0] == 1


@adrop_table_between_tests(table_name=TableNames.INDEX_ONLY_INSERT)
async def run_test_only_index_async(engine, schema, if_row_exists):
    # config
    table_name = TableNames.INDEX_ONLY_INSERT

    # upsert df with only index
    df = pd.DataFrame({'ix': [1]}).set_index('ix')
    await aupsert(con=engine, schema=schema, df=df, table_name=table_name, if_row_exists=if_row_exists)

    # check data integrity
    df_db = await aselect_table(engine=engine, schema=schema, table_name=table_name)
    assert 'ix' in df_db.columns
    assert len(df_db) > 0
    assert df_db['ix'].iloc[0] == 1


# -

# # Actual tests

# +
def test_create_and_insert_table_multiindex(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_create_and_insert_table_multiindex_async,
                       f_sync=run_test_create_and_insert_table_multiindex)


def test_index_with_null(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_index_with_null_async,
                       f_sync=run_test_index_with_null)


@pytest.mark.parametrize("if_row_exists", ['update', 'ignore'])
def test_only_index(engine, schema, if_row_exists):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_only_index_async,
                       f_sync=run_test_only_index,
                       if_row_exists=if_row_exists)
