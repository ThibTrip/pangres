#!/usr/bin/env python
# coding: utf-8
import pytest
import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
from sqlalchemy.sql.sqltypes import BIGINT, INTEGER, BIGINT, TEXT
from sqlalchemy.dialects.postgresql.base import DOUBLE_PRECISION, TIMESTAMP

from pangres.helpers import PandasSpecialEngine
from pangres.tests.conftest import TestDB


# # Config



testdb = TestDB()



# # Tests



def test_chain():
    """
    Makes tests in chain for PandasSpecialEngine.
    This makes code easier with a small inconvenient
    of not separating tests thematically.
    """
    df = pd.DataFrame({
    'empty_col': [None, None],
    'int': [1, 2],
    'float': [1.2, 2.3],
    'ts_tz': [
        pd.Timestamp('2019-12-01', tz='UTC'),
        pd.Timestamp('2019-08-01', tz='UTC')
        ],
        'text': ['foo', 'bar']
    }).rename_axis(['profileid'], axis='index', inplace=False)
    
    # TEST INIT
    table_name = 'test_pandas_special_engine'
    pse = PandasSpecialEngine(testdb.engine, 
                              df, 
                              table_name=table_name, 
                              schema=testdb.schema)
    
    # TEST ATTRIBUTE pse.df_col_names
    assert pse.df_col_names ==  ['profileid'] + df.columns.tolist()
    
    # TEST TABLE CREATION
    pse.create_schema_if_not_exists()
    pse.create_table_if_not_exists()

    # TEST INSERTION
    pse.insert(if_exists='upsert_overwrite')
    df_db = pd.read_sql(f'SELECT * FROM {testdb.schema}.{table_name}',
                        con=testdb.engine,
                        index_col='profileid')
    df_db['ts_tz'] = pd.to_datetime(df_db['ts_tz'], utc=True)
    pd.testing.assert_frame_equal(df, df_db)

    # TEST ADAPT DTYPES OF EMPTY COLUMNS
    # change data locally
    # fill column empty_col with some data
    df_test = df.assign(empty_col=[1, 2])

    # recreate instance of PandasSpecialEngine
    pse = PandasSpecialEngine(testdb.engine,
                              df_test,
                              table_name=table_name,
                              schema=testdb.schema)
    pse.adapt_dtype_of_empty_db_columns()

    # update values in database
    pse.insert(if_exists='upsert_overwrite')

    # check if empty_col is now of integer dtype
    assert pd.api.types.is_integer_dtype(
        pd.read_sql(f'SELECT "empty_col" FROM {testdb.schema}.{table_name}',
                    con=testdb.engine)['empty_col'].dtype)

    # TEST GET DB COLUMN NAMES
    assert pse.get_db_columns_names() == ['profileid'] + df.columns.tolist()

    # TEST GET DB COLUMN TYPES
    expected = {
        'profileid': BIGINT,
        'empty_col': INTEGER,
        'int': BIGINT,
        'float': DOUBLE_PRECISION,
        'ts_tz': TIMESTAMP,
        'text': TEXT
    }

    dtypes = pse.get_db_columns_types()
    dtypes = {col: type(dtype) for col, dtype in dtypes.items()}

    assert dtypes == expected

    # TEST ADD NEW COLUMNS
    # repeat df columns with the suffix "_new"
    df_test = pd.concat([df, df.rename(columns=lambda col: col + '_new')],
                        axis=1)

    # recreate instance of PandasSpecialEngine since we have a new df
    pse = PandasSpecialEngine(testdb.engine,
                              df_test,
                              table_name=table_name,
                              schema=testdb.schema)
    pse.adapt_dtype_of_empty_db_columns()

    # add new columns
    pse.add_new_columns()

    # update values
    pse.insert(if_exists='upsert_overwrite')

    # check the new columns are here and that their types match
    df_db = pd.read_sql(f'SELECT * FROM {testdb.schema}.{table_name}',
                        con=testdb.engine,
                        index_col='profileid')
    df_db['ts_tz'] = pd.to_datetime(df_db['ts_tz'], utc=True)
    df_db['ts_tz_new'] = pd.to_datetime(df_db['ts_tz_new'], utc=True)
    pd.testing.assert_frame_equal(df_test, df_db)


def test_errors():
    """
    Test errors raised by PandasSpecialEngine methods
    when possible.
    """
    # no index name
    df = pd.DataFrame({'test':[1]})
    with pytest.raises(IndexError) as excinfo:
        pse = PandasSpecialEngine(testdb.engine,
                                  df,
                                  table_name='irrelevant',
                                  schema=testdb.schema)
    assert "All index levels must be named" in str(excinfo.value)

    # duplicates names amonst columns/index levels
    df = pd.DataFrame({'test':[1]})
    df.index.name = 'test'
    with pytest.raises(ValueError) as excinfo:
        pse = PandasSpecialEngine(testdb.engine,
                                  df,
                                  table_name='irrelevant',
                                  schema=testdb.schema)
    assert "There cannot be duplicated names" in str(excinfo.value)

    # non unique index
    df = pd.DataFrame({'test':[1, 2]})
    df.index = [0, 0]
    df.index.name = 'test' # do this last as the line above would delete the name
    with pytest.raises(IndexError) as excinfo:
        pse = PandasSpecialEngine(testdb.engine,
                                  df,
                                  table_name='irrelevant',
                                  schema=testdb.schema)
    assert "The index must be unique" in str(excinfo.value)

    # forbidden characters of pangres in column names
    df = pd.DataFrame({'test()':[1], 'test%':[1]})
    df.index.name = 'test'
    with pytest.raises(ValueError) as excinfo:
        pse = PandasSpecialEngine(testdb.engine,
                                  df,
                                  table_name='irrelevant',
                                  schema=testdb.schema,
                                  clean_column_names=False)
    assert "forbidden character" in str(excinfo.value)
    
    # provide dumb chunksize in
    # PandasSpecialEngine._split_list_in_chunks
    # (function used for splitting values in chunks before upserting)
    df = pd.DataFrame({'test':[1]})
    df.index.name = 'profileid'
    pse = PandasSpecialEngine(testdb.engine,
                              df,
                              table_name='irrelevant',
                              schema=testdb.schema,
                              clean_column_names=False)
    # chunksize should be an integer above 0
    for dumb_value in (0, -1, 1.2):
        with pytest.raises(ValueError) as excinfo:
            pse._split_list_in_chunks(['a', 'b', 'c'], chunksize=dumb_value)
        assert "integer strictly above 0" in str(excinfo.value)