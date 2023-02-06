#!/usr/bin/env python
# coding: utf-8
# +
"""
This module tests if uploading data in chunks works as
expected (we should get the correct DataFrame length back).
"""
import pandas as pd
import pytest

# local imports
from pangres import aupsert, upsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import adrop_table_between_tests, drop_table_between_tests, sync_or_async_test, TableNames


# -

# # Sync and async variants for tests
#
# (`run_test_foo`|`run_test_foo_async`) -> `test_foo`

# +
@drop_table_between_tests(table_name=TableNames.VARIOUS_CHUNKSIZES)
def run_test_various_chunksizes(engine, schema, chunksize, nb_rows):
    # get example df
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows)

    # MySQL does not want flexible text length in indices/PK
    upsert(con=engine, schema=schema, table_name=TableNames.VARIOUS_CHUNKSIZES,
           df=df, chunksize=chunksize, if_row_exists='update')
    df_db = _TestsExampleTable.read_from_db(engine=engine, schema=schema, table_name=TableNames.VARIOUS_CHUNKSIZES)

    # sort index (for MySQL...)
    pd.testing.assert_frame_equal(df.sort_index(), df_db.sort_index())


@adrop_table_between_tests(table_name=TableNames.VARIOUS_CHUNKSIZES)
async def run_test_various_chunksizes_async(engine, schema, chunksize, nb_rows):
    # get example df
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows)

    # MySQL does not want flexible text length in indices/PK
    await aupsert(con=engine, schema=schema, table_name=TableNames.VARIOUS_CHUNKSIZES,
                  df=df, chunksize=chunksize, if_row_exists='update')
    df_db = await _TestsExampleTable.aread_from_db(engine=engine, schema=schema,
                                                   table_name=TableNames.VARIOUS_CHUNKSIZES)

    # sort index (for MySQL...)
    pd.testing.assert_frame_equal(df.sort_index(), df_db.sort_index())


# -

# # Actual tests

@pytest.mark.parametrize('chunksize, nb_rows', [[1, 11], [3, 11]], ids=['one_by_one', 'odd_chunksize'])
def test_insert_various_chunksizes(engine, schema, chunksize, nb_rows):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_various_chunksizes_async,
                       f_sync=run_test_various_chunksizes,
                       chunksize=chunksize,
                       nb_rows=nb_rows)
