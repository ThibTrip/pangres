#!/usr/bin/env python
# coding: utf-8
"""
This module tests if uploading data in chunks works as
expected (we should get the correct DataFrame length back).
"""
import pandas as pd
import pytest
from sqlalchemy import VARCHAR
from pangres import upsert
from pangres.examples import _TestsExampleTable
from pangres.tests.conftest import drop_table_for_test, read_example_table_from_db, TableNames


# # Helpers

@drop_table_for_test(TableNames.VARIOUS_CHUNKSIZES)
def insert_chunks(engine, schema, chunksize, nb_rows):
    df = _TestsExampleTable.create_example_df(nb_rows=nb_rows)
    # MySQL does not want flexible text length in indices/PK
    dtype = {'profileid':VARCHAR(10)} if 'mysql' in engine.dialect.dialect_description else None
    upsert(schema=schema, table_name=TableNames.VARIOUS_CHUNKSIZES, df=df, chunksize=chunksize,
           con=engine, if_row_exists='update', dtype=dtype)
    df_db = read_example_table_from_db(engine=engine, schema=schema, table_name=TableNames.VARIOUS_CHUNKSIZES)
    # sort index (for MySQL...)
    pd.testing.assert_frame_equal(df.sort_index(), df_db.sort_index())


# # Tests

@pytest.mark.parametrize('chunksize, nb_rows', [[1, 11], [3, 11]], ids=['one_by_one', 'odd_chunksize'])
def test_insert_various_chunksizes(engine, schema, chunksize, nb_rows):
    insert_chunks(engine, schema, chunksize=chunksize, nb_rows=nb_rows)
