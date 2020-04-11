#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import pytest
from sqlalchemy import VARCHAR
from pangres.examples import _TestsExampleTable
from pangres.helpers import PandasSpecialEngine
from pangres.tests.conftest import drop_table_if_exists


# # Test methods and attributes

def test_methods_and_attributes(engine, schema):
    """
    Makes tests in chain for PandasSpecialEngine.
    This makes code easier with a small inconvenient
    of not separating tests thematically.
    """
    # dtype for index for MySQL... (can't have flexible text length)
    dtype = {'profileid':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    table_name = 'test_pandas_special_engine'
    default_args = {'engine':engine,
                    'schema':schema,
                    'dtype':dtype,
                    'table_name':table_name}
    drop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    # TEST INIT
    df = _TestsExampleTable.create_example_df(nb_rows=10)
    pse = PandasSpecialEngine(df=df, **default_args)
    # TEST ATTRIBUTE pse.table
    expected_cols = list(df.index.names) + df.columns.tolist()
    assert all((col in pse.table.columns for col in expected_cols))  
    # TEST TABLE AND SCHEMA CREATION
    pse.create_schema_if_not_exists()
    pse.create_table_if_not_exists()

    # TEST ADD NEW COLUMNS
    # don't try to add JSON columns!
    # It's not supported by sqlalchemy compilers :(
    df = df.assign(new_text_col='test',
                   new_int_col=0,
                   new_float_col=1.1,
                   new_bool_col=False,
                   new_dt_col=pd.Timestamp('2020-01-01'),
                   # create this col for later
                   empty_col=None)
    # recreate pse then add columns
    pse = PandasSpecialEngine(df=df, **default_args)
    pse.add_new_columns()
    
    # TEST CHANGE COLUMN TYPE (not for SQlite)
    if not pse._db_type == 'sqlite':
        # don't try to alter from any type to JSON!
        # It's not supported by sqlalchemy compilers :(
        # also the order is very specific!!! we have to cast types
        # even though the column is empty (to avoid losing column information such as constraint)
        # so e.g. casting from BOOLEAN to BIGINT is not possible
        # actually BOOLEAN is not even here because it breaks the chain
        alterations = (1, 1.1, "abc", pd.Timestamp("2020-01-01", tz='UTC'))
        for i in alterations:
            # change empty_col
            df['empty_col'] = df['empty_col'].map(lambda x: i) # this will work for lists or dicts as well
            # recreate pse then change column type
            pse = PandasSpecialEngine(df=df, **default_args)
            pse.adapt_dtype_of_empty_db_columns()


# # Test errors

def test_errors(engine, schema):
    """
    Test errors raised by PandasSpecialEngine methods
    when possible.
    """
    table_name = 'test_pandas_special_engine_errors'
    default_args = {'engine':engine,
                    'schema':schema,
                    # this is most likely irrelevant (no table shall be created)
                    'table_name':table_name}
    drop_table_if_exists(engine=engine, schema=schema, table_name=table_name)
    # no index name
    df = pd.DataFrame({'test':[1]})
    with pytest.raises(IndexError) as excinfo:
        pse = PandasSpecialEngine(df=df, **default_args)
    assert "All index levels must be named" in str(excinfo.value)

    # duplicates names amonst columns/index levels
    df = pd.DataFrame({'test':[1]})
    df.index.name = 'test'
    with pytest.raises(ValueError) as excinfo:
        pse = PandasSpecialEngine(df=df, **default_args)
    assert "There cannot be duplicated names" in str(excinfo.value)

    # non unique index
    df = pd.DataFrame({'test':[1, 2]}).assign(index=[0,0]).set_index('index')
    with pytest.raises(IndexError) as excinfo:
        pse = PandasSpecialEngine(df=df, **default_args)
    assert "The index must be unique" in str(excinfo.value)

    # provide dumb chunksize in
    # PandasSpecialEngine._get_values_to_insert
    # create a dummy PandasSpecialEngine
    df = pd.DataFrame({'test':[1]})
    df.index.name = 'profileid'
    pse = PandasSpecialEngine(df=df, **default_args)
    # chunksize should be an integer above 0
    for dumb_value in (0, -1, 1.2):
        with pytest.raises(ValueError) as excinfo:
            pse._create_chunks(values=[0], chunksize=dumb_value)
        assert "integer strictly above 0" in str(excinfo.value)
