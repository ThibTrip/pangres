#!/usr/bin/env python
# coding: utf-8
import datetime
import logging
import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text, VARCHAR
from sqlalchemy.sql.elements import Null as SqlaNull
from pangres.examples import _TestsExampleTable
from pangres.exceptions import (DuplicateLabelsException,
                                DuplicateValuesInIndexException,
                                HasNoSchemaSystemException,
                                MissingIndexLevelInSqlException,
                                UnnamedIndexLevelsException)
from pangres.engine import PandasSpecialEngine
from pangres.helpers import _sqla_gt14
from pangres.tests.conftest import (commit, drop_table_for_test,
                                    get_table_namespace,
                                    schema_for_testing_creation,
                                    TableNames)


# # Test methods and attributes

# +
@drop_table_for_test(TableNames.NO_TABLE)
def test_repr(engine, schema):
    dummy_df = pd.DataFrame(index=pd.Index(data=[0], name='id'))
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, schema=schema,
                                  table_name=TableNames.NO_TABLE, df=dummy_df)
        # make sure it is printable without errors
        txt = str(pse)
        print(txt)
        # test some strings we expect to find in the repr
        for s in ('PandasSpecialEngine', 'id ', 'hexid', 'connection',
                  'schema', 'table', 'SQLalchemy table model'):
            assert s in txt


@drop_table_for_test(TableNames.NO_TABLE)
def test_table_attr(engine, schema):
    # generate a somewhat complex table model via the _TestsExampleTable class
    df = _TestsExampleTable.create_example_df(nb_rows=10)
    table_name = TableNames.NO_TABLE
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, schema=schema,
                                  table_name=table_name, df=df)
        # make sure columns and table name match
        expected_cols = list(df.index.names) + df.columns.tolist()
        assert all((col in pse.table.columns for col in expected_cols))
        assert pse.table.name == table_name


@drop_table_for_test(TableNames.NO_TABLE)
def test_schema_creation(engine, schema):
    # overwrite schema
    schema = schema_for_testing_creation

    # local helper
    def drop_schema():
        if 'postgres' in engine.dialect.dialect_description:
            with engine.connect() as connection:
                connection.execute(text(f'DROP SCHEMA IF EXISTS {schema};'))
                commit(connection)

    # schema may already exist before testing
    drop_schema()

    # then try to create a schema from a PandasSpecialEngine instance
    dummy_df = pd.DataFrame(index=pd.Index(data=[0], name='id'))
    with engine.connect() as connection:
        try:
            pse = PandasSpecialEngine(connection=connection, schema=schema,
                                      table_name=TableNames.NO_TABLE, df=dummy_df)
            # this should raise HasNoSchemaSystemException
            # if we are not on a postgres engine
            assert not pse.schema_exists()
            pse.create_schema_if_not_exists()
            commit(connection)
            assert pse.schema_exists()
        except Exception as e:
            if pse._db_type == 'postgres' or not isinstance(e, HasNoSchemaSystemException):
                raise e
        finally:
            if pse._db_type == 'postgres':
                drop_schema()


@drop_table_for_test(TableNames.TABLE_CREATION)
def test_table_creation(engine, schema):
    dtype = {'profileid':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None
    df = _TestsExampleTable.create_example_df(nb_rows=10)
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, schema=schema, dtype=dtype,
                                  table_name=TableNames.TABLE_CREATION, df=df)
        assert not pse.table_exists()
        pse.create_table_if_not_exists()
        commit(connection)
        assert pse.table_exists()


@pytest.mark.parametrize('axis', ['index', 'column'])
@drop_table_for_test(TableNames.ADD_NEW_COLUMN)
def test_add_new_columns(engine, schema, axis):
    # store arguments we will use for multiple PandasSpecialEngine instances
    table_name = TableNames.ADD_NEW_COLUMN
    common_kwargs = dict(schema=schema, table_name=table_name)
    common_kwargs['dtype'] = {'profileid':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None

    # create our example table
    df = _TestsExampleTable.create_example_df(nb_rows=10)
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, df=df, **common_kwargs)
        pse.create_table_if_not_exists()
        commit(connection)
        assert pse.table_exists()

    # we need to recreate an instance of PandasSpecialEngine
    # so that a new table model with the new columns is created then add columns
    with engine.connect() as connection:
        # error message if we get unexpected values for "axis"
        # or we make a typo in our if/elif statements
        err_msg = f'Expected axis to be one of index, columns. Got {axis}'
        # add a new index level or new columns (no JSON ones,
        # it's not supported by sqlalchemy compilers :( )
        if axis == 'index':
            df['new_index_col'] = 'foo'
            df.set_index('new_index_col', append=True, inplace=True)
        elif axis == 'column':
            df = df.assign(new_text_col='test',
                           new_int_col=0,
                           new_float_col=1.1,
                           new_bool_col=False,
                           new_dt_col=pd.Timestamp('2020-01-01'),
                           # create this col for later
                           empty_col=None)
        else:
            raise AssertionError(err_msg)

        # recreate PandasSpecialEngine
        pse = PandasSpecialEngine(connection=connection, df=df, **common_kwargs)

        # check if we get an error when trying to add an index level
        if axis == 'index':
            with pytest.raises(MissingIndexLevelInSqlException) as exc_info:
                pse.add_new_columns()
            assert 'Cannot add' in str(exc_info.value)
            return
        elif axis == 'column':
            pse.add_new_columns()
            commit(connection)
        else:
            raise AssertionError(err_msg)

    # check if the columns were correctly added
    # since we issued a return for 'index' earlier
    # the axis must now be 'columns'
    assert axis == 'column'
    # check the columns where added
    with engine.connect() as connection:
        ns = get_table_namespace(schema=schema, table_name=table_name)
        df_db = pd.read_sql(text(f'SELECT * FROM {ns} LIMIT 0;'), con=connection,
                            index_col='profileid')
        assert set(df.columns) == set(df_db.columns)


@pytest.mark.parametrize("new_empty_column_value", [1, 1.1, pd.Timestamp("2020-01-01", tz='UTC'),
                                                    {'foo':'bar'}, ['foo'], True])
@drop_table_for_test(TableNames.CHANGE_EMPTY_COL_TYPE)
def test_change_column_type_if_column_empty(engine, schema, caplog, new_empty_column_value):
    # store arguments we will use for multiple PandasSpecialEngine instances
    table_name = TableNames.CHANGE_EMPTY_COL_TYPE
    common_kwargs = dict(schema=schema, table_name=table_name)
    common_kwargs['dtype'] = {'profileid':VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None

    # json like will not work for sqlalchemy < 1.4
    # also skip sqlite as it does not support such alteration
    json_like = isinstance(new_empty_column_value, (dict, list)) and not _sqla_gt14()
    if json_like or 'sqlite' in engine.dialect.dialect_description:
        pytest.skip()

    # create our example table
    df = pd.DataFrame({'profileid':['foo'], 'empty_col':[None]}).set_index('profileid')
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, df=df, **common_kwargs)
        pse.create_table_if_not_exists()
        commit(connection)
        assert pse.table_exists()

    # recreate an instance of PandasSpecialEngine with a new df (so the model gets refreshed)
    # the line below is a "hack" to set any type of element as a column value
    # without pandas trying to broadcast it. This is useful when passing a list or such
    df['empty_col'] = df.index.map(lambda x: new_empty_column_value)
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, df=df, **common_kwargs)
        with caplog.at_level(logging.INFO, logger='pangres'):
            pse.adapt_dtype_of_empty_db_columns()
        assert len(caplog.records) == 1
        assert 'Changed type of column empty_col' in caplog.text


def test_values_conversion(_):
    engine = create_engine('sqlite:///')
    row = {'id':0,
           'pd_interval':pd.Interval(left=0, right=5),
           'nan':np.nan,
           'nat':pd.NaT,
           'none':None,
           'pd_na':getattr(pd, 'NA', None),
           'ts':pd.Timestamp('2021-01-01')}
    df = pd.DataFrame([row]).set_index('id')
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, df=df, table_name=TableNames.NO_TABLE)
        values = pse._get_values_to_insert()
    converted_row = values[0]
    assert len(row) == len(converted_row)
    # iterate over the keys of the row and get the index (after conversion we just have a list)
    for ix, k in enumerate(row):
        v = row[k]
        v_converted = converted_row[ix]
        if k == 'id':
            assert v_converted == v
        elif k == 'pd_interval':
            assert isinstance(v_converted, str)
        elif k == 'ts':
            assert isinstance(v_converted, datetime.datetime)
        # we should receive all null likes here
        else:
            assert pd.isna(v)
            assert isinstance(v_converted, SqlaNull)


# dummy connection string to test our categorization for databases
params_db_type_tests = [('sqlite:///', 'sqlite'),
                        ('postgresql+psycopg2://username:password@localhost:5432/postgres', 'postgres'),
                        ('postgresql://username:password@localhost:5432/postgres', 'postgres'),
                        ('mysql+pymysql://username:password@localhost:3306/db', 'mysql'),
                        ('oracle+cx_oracle://username:password@localhost', 'other')]


@pytest.mark.parametrize("connection_string, expected", params_db_type_tests)
def test_detect_db_type(_, connection_string, expected):
    engine = create_engine(connection_string)
    assert PandasSpecialEngine._detect_db_type(connectable=engine) == expected


# -

# # Test errors

# +
@drop_table_for_test(TableNames.NO_TABLE)
def test_error_index_level_named(engine, schema):
    df = pd.DataFrame({'test':[0]})
    with pytest.raises(UnnamedIndexLevelsException) as excinfo:
        with engine.connect() as connection:
            PandasSpecialEngine(connection=connection, schema=schema, table_name=TableNames.NO_TABLE, df=df)
    assert "All index levels must be named" in str(excinfo.value)


@pytest.mark.parametrize("option", ['index and column collision', 'columns duplicated', 'index duplicated'])
@drop_table_for_test(TableNames.NO_TABLE)
def test_duplicated_names(engine, schema, option):
    df = pd.DataFrame({'test':[0]})
    if option == 'index and column collision':
        df.index.name = 'test'
    elif option == 'columns duplicated':
        df.index.name = 'ix'
        df = df[['test', 'test']]
    elif option == 'index duplicated':
        df = df.set_index(['test', 'test'])
    else:
        raise AssertionError(f'Unexpected value for param `option`: {option}')

    with pytest.raises(DuplicateLabelsException) as excinfo:
        with engine.connect() as connection:
            PandasSpecialEngine(connection=connection, schema=schema, table_name=TableNames.NO_TABLE, df=df)
    assert "Found duplicates across index and columns" in str(excinfo.value)


@drop_table_for_test(TableNames.NO_TABLE)
def test_non_unique_index(engine, schema):
    df = pd.DataFrame(index=pd.Index(data=[0, 0], name='ix'))
    with pytest.raises(DuplicateValuesInIndexException) as excinfo:
        with engine.connect() as connection:
            PandasSpecialEngine(connection=connection, schema=schema, table_name=TableNames.NO_TABLE, df=df)
    assert "The index must be unique" in str(excinfo.value)


@pytest.mark.parametrize("bad_chunksize_value", [0, -1, 1.2])
@drop_table_for_test(TableNames.NO_TABLE)
def test_bad_chunksize(engine, schema, bad_chunksize_value):
    df = pd.DataFrame({'test':[0]})
    df.index.name = 'id'
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, schema=schema, table_name=TableNames.NO_TABLE, df=df)
        with pytest.raises(ValueError) as excinfo:
            pse._create_chunks(values=[0], chunksize=bad_chunksize_value)
        assert "integer strictly above 0" in str(excinfo.value)
