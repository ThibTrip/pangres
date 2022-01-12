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
from pangres.tests.conftest import drop_table_for_test, get_table_namespace, TableNames


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


# -

# # Tests

# ## Test MultiIndex

@drop_table_for_test(TableNames.MULTIINDEX)
def test_create_and_insert_table_multiindex(engine, schema):
    # local helpers
    table_name = TableNames.MULTIINDEX
    namespace = get_table_namespace(schema=schema, table_name=table_name)

    def read_from_db():
        with engine.connect() as connection:
            df = pd.read_sql(text(f'SELECT * FROM {namespace}'), con=connection)
            df['ix3'] = pd.to_datetime(df['ix3'])
            return df.set_index(index_col)

    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'ix2':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None

    # create
    upsert(con=engine, schema=schema, df=df_multiindex, table_name=table_name, dtype=dtype, if_row_exists='update')
    df_db = read_from_db()
    pd.testing.assert_frame_equal(df_db, df_multiindex)

    # insert
    upsert(con=engine, schema=schema, df=df_multiindex2, table_name=table_name, dtype=dtype, if_row_exists='update')
    df_db = read_from_db()
    pd.testing.assert_frame_equal(df_db, pd.concat(objs=[df_multiindex, df_multiindex2]))


# ## Test index with null value

@drop_table_for_test(TableNames.INDEX_WITH_NULL)
def test_index_with_null(engine, schema):
    df = pd.DataFrame({'ix':[None, 0], 'test': [0, 1]}).set_index('ix')
    with pytest.raises(IntegrityError):
        upsert(con=engine, schema=schema, df=df, table_name=TableNames.INDEX_WITH_NULL, if_row_exists='update')
        # don't test error for mysql since only a warning is raised and the line is skipped
        if 'mysql' in engine.dialect.dialect_description:
            pytest.skip()


# ## Test only index

@pytest.mark.parametrize("if_row_exists", ['update', 'ignore'])
@drop_table_for_test(TableNames.INDEX_ONLY_INSERT)
def test_only_index(engine, schema, if_row_exists):
    # config
    table_name = TableNames.INDEX_ONLY_INSERT

    # upsert df with only index
    df = pd.DataFrame({'ix':[1]}).set_index('ix')
    upsert(con=engine, schema=schema, df=df, table_name=table_name, if_row_exists=if_row_exists)

    # check data integrity
    namespace = get_table_namespace(schema=schema, table_name=table_name)
    with engine.connect() as connection:
        df_db = pd.read_sql(text(f'SELECT * FROM {namespace}'), con=connection)
    assert 'ix' in df_db.columns
    assert len(df_db) > 0
    assert df_db['ix'].iloc[0] == 1
