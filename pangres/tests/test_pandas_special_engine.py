#!/usr/bin/env python
# coding: utf-8
# +
import datetime
import logging
import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, VARCHAR
from sqlalchemy.sql.elements import Null as SqlaNull

# local imports
from pangres.examples import _TestsExampleTable
from pangres.exceptions import (DuplicateLabelsException,
                                DuplicateValuesInIndexException,
                                HasNoSchemaSystemException,
                                MissingIndexLevelInSqlException,
                                UnnamedIndexLevelsException)
from pangres.engine import PandasSpecialEngine
from pangres.helpers import _sqla_gt14
from pangres.tests.conftest import (adrop_schema, adrop_table_between_tests,
                                    commit, create_sync_or_async_engine,
                                    drop_table_between_tests, drop_schema,
                                    schema_for_testing_creation,
                                    sync_or_async_test, TableNames)


# -

# # Sync and async variants for tests
#
# (`run_test_foo`|`run_test_foo_async`) -> `test_foo`

# ## Schema creation
#
# We do not create any table here

# +
def run_test_schema_creation(engine, schema):
    # overwrite schema
    schema = schema_for_testing_creation

    # schema may already exist before testing
    if 'postgres' in engine.dialect.dialect_description:
        drop_schema(engine=engine, schema=schema)

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
                raise e  # pragma: no cover
        finally:
            if pse._db_type == 'postgres':
                drop_schema(engine=engine, schema=schema)


async def run_test_schema_creation_async(engine, schema):
    # overwrite schema
    schema = schema_for_testing_creation

    # schema may already exist before testing
    if 'postgres' in engine.dialect.dialect_description:
        await adrop_schema(engine=engine, schema=schema)

    # then try to create a schema from a PandasSpecialEngine instance
    dummy_df = pd.DataFrame(index=pd.Index(data=[0], name='id'))
    async with engine.connect() as connection:
        try:
            pse = PandasSpecialEngine(connection=connection, schema=schema,
                                      table_name=TableNames.NO_TABLE, df=dummy_df)
            # this should raise HasNoSchemaSystemException
            # if we are not on a postgres engine
            assert not await pse.aschema_exists()
            await pse.acreate_schema_if_not_exists()
            await connection.commit()
            assert await pse.aschema_exists()
        except Exception as e:
            if pse._db_type == 'postgres' or not isinstance(e, HasNoSchemaSystemException):
                raise e  # pragma: no cover
        finally:
            if pse._db_type == 'postgres':
                await adrop_schema(engine=engine, schema=schema)


# -

# ## Table creation

# +
@drop_table_between_tests(table_name=TableNames.TABLE_CREATION)
def run_test_table_creation(engine, schema):
    df = _TestsExampleTable.create_example_df(nb_rows=10)
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, schema=schema,
                                  table_name=TableNames.TABLE_CREATION, df=df)
        assert not pse.table_exists()
        pse.create_table_if_not_exists()
        commit(connection)
        assert pse.table_exists()


@adrop_table_between_tests(table_name=TableNames.TABLE_CREATION)
async def run_test_table_creation_async(engine, schema):
    df = _TestsExampleTable.create_example_df(nb_rows=10)
    async with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, schema=schema,
                                  table_name=TableNames.TABLE_CREATION, df=df)
        assert not await pse.atable_exists()
        await pse.acreate_table_if_not_exists()
        await connection.commit()
        assert await pse.atable_exists()


# -

# ## Adding new columns

# +
@drop_table_between_tests(table_name=TableNames.ADD_NEW_COLUMN)
def run_test_add_new_columns(engine, schema, on_index: bool):
    # store arguments we will use for multiple PandasSpecialEngine instances
    table_name = TableNames.ADD_NEW_COLUMN
    common_kwargs = dict(schema=schema, table_name=table_name)

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
        # add a new index level or new columns (no JSON ones,
        # it's not supported by sqlalchemy compilers :( )
        if on_index:
            df['new_index_col'] = 'foo'
            df.set_index('new_index_col', append=True, inplace=True)
        else:
            df = df.assign(new_text_col='test',
                           new_int_col=0,
                           new_float_col=1.1,
                           new_bool_col=False,
                           new_dt_col=pd.Timestamp('2020-01-01'),
                           # create this col for later
                           empty_col=None)

        # recreate PandasSpecialEngine
        pse = PandasSpecialEngine(connection=connection, df=df, **common_kwargs)

        # check if we get an error when trying to add an index level
        if on_index:
            with pytest.raises(MissingIndexLevelInSqlException) as exc_info:
                pse.add_new_columns()
            assert 'Cannot add' in str(exc_info.value)
            return
        else:
            pse.add_new_columns()
            commit(connection)

    # check if the columns were correctly added
    assert not on_index
    with engine.connect() as connection:
        df_db = _TestsExampleTable.read_from_db(engine=engine, schema=schema, table_name=table_name)
        assert set(df.columns) == set(df_db.columns)


@adrop_table_between_tests(table_name=TableNames.ADD_NEW_COLUMN)
async def run_test_add_new_columns_async(engine, schema, on_index: bool):
    # store arguments we will use for multiple PandasSpecialEngine instances
    table_name = TableNames.ADD_NEW_COLUMN
    common_kwargs = dict(schema=schema, table_name=table_name)

    # create our example table
    df = _TestsExampleTable.create_example_df(nb_rows=10)
    async with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, df=df, **common_kwargs)
        await pse.acreate_table_if_not_exists()
        await connection.commit()
        assert await pse.atable_exists()

    # we need to recreate an instance of PandasSpecialEngine
    # so that a new table model with the new columns is created then add columns
    async with engine.connect() as connection:
        # add a new index level or new columns (no JSON ones,
        # it's not supported by sqlalchemy compilers :( )
        if on_index:
            df['new_index_col'] = 'foo'
            df.set_index('new_index_col', append=True, inplace=True)
        else:
            df = df.assign(new_text_col='test',
                           new_int_col=0,
                           new_float_col=1.1,
                           new_bool_col=False,
                           new_dt_col=pd.Timestamp('2020-01-01'),
                           # create this col for later
                           empty_col=None)

        # recreate PandasSpecialEngine with the new df
        pse = PandasSpecialEngine(connection=connection, df=df, **common_kwargs)

        # check if we get an error when trying to add an index level
        if on_index:
            with pytest.raises(MissingIndexLevelInSqlException) as exc_info:
                await pse.aadd_new_columns()
            assert 'Cannot add' in str(exc_info.value)
            return
        else:
            await pse.aadd_new_columns()
            await connection.commit()

    # check if the columns were correctly added
    assert not on_index
    async with engine.connect() as connection:
        df_db = await _TestsExampleTable.aread_from_db(engine=engine, schema=schema, table_name=table_name)
        assert set(df.columns) == set(df_db.columns)


# -

# ## Changing data type for empty columns

# +
@drop_table_between_tests(table_name=TableNames.CHANGE_EMPTY_COL_TYPE)
def run_test_change_column_type_if_column_empty(engine, schema, caplog, new_empty_column_value):
    # store arguments we will use for multiple PandasSpecialEngine instances
    table_name = TableNames.CHANGE_EMPTY_COL_TYPE
    common_kwargs = dict(schema=schema, table_name=table_name)
    common_kwargs['dtype'] = {'profileid': VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None

    # json like will not work for sqlalchemy < 1.4
    # also skip sqlite as it does not support such alteration
    json_like = isinstance(new_empty_column_value, (dict, list))
    if json_like and not _sqla_gt14():
        pytest.skip('JSON like values will not work for sqlalchemy < 1.4')  # pragma: no cover
    elif 'sqlite' in engine.dialect.dialect_description:
        pytest.skip('such column alteration is not possible with SQlite')

    # create our example table
    df = pd.DataFrame({'profileid': ['foo'], 'empty_col': [None]}).set_index('profileid')
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
    caplog.clear()


@adrop_table_between_tests(table_name=TableNames.CHANGE_EMPTY_COL_TYPE)
async def run_test_change_column_type_if_column_empty_async(engine, schema, caplog, new_empty_column_value):
    # store arguments we will use for multiple PandasSpecialEngine instances
    table_name = TableNames.CHANGE_EMPTY_COL_TYPE
    common_kwargs = dict(schema=schema, table_name=table_name)
    common_kwargs['dtype'] = {'profileid': VARCHAR(5)} if 'mysql' in engine.dialect.dialect_description else None

    # json like will not work for sqlalchemy < 1.4
    # also skip sqlite as it does not support such alteration
    json_like = isinstance(new_empty_column_value, (dict, list))
    if json_like and not _sqla_gt14():
        pytest.skip('JSON like values will not work for sqlalchemy < 1.4')  # pragma: no cover
    elif 'sqlite' in engine.dialect.dialect_description:
        pytest.skip('such column alteration is not possible with SQlite')

    # create our example table
    df = pd.DataFrame({'profileid': ['foo'], 'empty_col': [None]}).set_index('profileid')
    async with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, df=df, **common_kwargs)
        await pse.acreate_table_if_not_exists()
        await connection.commit()
        assert await pse.atable_exists()

    # recreate an instance of PandasSpecialEngine with a new df (so the model gets refreshed)
    # the line below is a "hack" to set any type of element as a column value
    # without pandas trying to broadcast it. This is useful when passing a list or such
    df['empty_col'] = df.index.map(lambda x: new_empty_column_value)
    async with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, df=df, **common_kwargs)
        with caplog.at_level(logging.INFO, logger='pangres'):
            await pse.aadapt_dtype_of_empty_db_columns()
        assert len(caplog.records) == 1
        assert 'Changed type of column empty_col' in caplog.text
    caplog.clear()


# -

# # Actual tests

# ## SQL flavor dependant tests

# +
def test_schema_creation(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_schema_creation_async,
                       f_sync=run_test_schema_creation)


def test_table_creation(engine, schema):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_table_creation_async,
                       f_sync=run_test_table_creation)


@pytest.mark.parametrize('on_index', [True, False], ids=['in df index', 'not in df index'])
def test_add_new_columns(engine, schema, on_index):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_add_new_columns_async,
                       f_sync=run_test_add_new_columns,
                       on_index=on_index)


params_new_value_empty_col = [1, 1.1, pd.Timestamp("2020-01-01", tz='UTC'), {'foo': 'bar'}, ['foo'], True]


@pytest.mark.parametrize(argnames="new_empty_column_value", argvalues=params_new_value_empty_col,
                         ids=[f'new_value={p}' for p in params_new_value_empty_col])
def test_change_column_type_if_column_empty(engine, schema, caplog, new_empty_column_value):
    sync_or_async_test(engine=engine, schema=schema,
                       f_async=run_test_change_column_type_if_column_empty_async,
                       f_sync=run_test_change_column_type_if_column_empty,
                       caplog=caplog, new_empty_column_value=new_empty_column_value)


# -

# ## SQL flavor agnostic tests

# +
def test_values_conversion(_):
    engine = create_engine('sqlite:///')
    row = {'id': 0,
           'pd_interval': pd.Interval(left=0, right=5),
           'nan': np.nan,
           'nat': pd.NaT,
           'none': None,
           'pd_na': getattr(pd, 'NA', None),
           'ts': pd.Timestamp('2021-01-01')}
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


# dummy connection strings to test our categorization for databases
params_db_type_tests = [('sqlite://', 'sqlite'),
                        ('sqlite+aiosqlite://', 'sqlite'),
                        ('postgresql+psycopg2://username:password@localhost:5432/postgres', 'postgres'),
                        ('postgresql://username:password@localhost:5432/postgres', 'postgres'),
                        ('mysql+pymysql://username:password@localhost:3306/db', 'mysql'),
                        ('mysql+aiomysql://username:password@localhost:3306/db', 'mysql'),
                        ('oracle+cx_oracle://username:password@localhost', 'other')]


@pytest.mark.parametrize("connection_string, expected", params_db_type_tests)
def test_detect_db_type(_, connection_string, expected):
    # there are some engines that we will not be able to create because they are asynchronous
    # and this requires sqlalchemy >= 1.4
    try:
        engine = create_sync_or_async_engine(connection_string)
    except NotImplementedError as e:  # pragma: no cover
        pytest.skip(str(e))
    except ModuleNotFoundError as e:  # pragma: no cover
        raise ModuleNotFoundError('There seems to be a dependency missing for this test. Please install it.') from e
    assert PandasSpecialEngine._detect_db_type(connectable=engine) == expected


def test_repr(_):
    engine = create_engine('sqlite://')
    dummy_df = pd.DataFrame(index=pd.Index(data=[0], name='id'))
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, table_name=TableNames.NO_TABLE, df=dummy_df)
        # make sure it is printable without errors
        txt = str(pse)
        print(txt)
        # test some strings we expect to find in the repr
        for s in ('PandasSpecialEngine', 'id ', 'hexid', 'connection',
                  'schema', 'table', 'SQLalchemy table model'):
            assert s in txt


def test_table_attr(_):
    engine = create_engine('sqlite://')
    # generate a somewhat complex table model via the _TestsExampleTable class
    df = _TestsExampleTable.create_example_df(nb_rows=10)
    table_name = TableNames.NO_TABLE
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, table_name=table_name, df=df)
        # make sure columns and table name match
        expected_cols = list(df.index.names) + df.columns.tolist()
        assert all((col in pse.table.columns for col in expected_cols))
        assert pse.table.name == table_name


# -

# ### Test errors

# +
@pytest.mark.parametrize("multi_index", [True, False], ids=['multi index', 'single index'])
def test_error_index_level_no_name(_, multi_index):
    engine = create_engine('sqlite://')
    df = pd.DataFrame({'test': [0]})
    if multi_index:
        df.set_index('test', append=True, inplace=True)
    with pytest.raises(UnnamedIndexLevelsException) as excinfo:
        with engine.connect() as connection:
            PandasSpecialEngine(connection=connection, table_name=TableNames.NO_TABLE, df=df)
    assert "All index levels must be named" in str(excinfo.value)


@pytest.mark.parametrize("option", ['index and column collision', 'columns duplicated', 'index duplicated'])
def test_duplicated_names(_, option):
    engine = create_engine('sqlite://')
    df = pd.DataFrame({'test': [0]})
    if option == 'index and column collision':
        df.index.name = 'test'
    elif option == 'columns duplicated':
        df.index.name = 'ix'
        df = df[['test', 'test']]
    elif option == 'index duplicated':
        df = df.set_index(['test', 'test'])
    else:  # pragma: no cover
        raise AssertionError(f'Unexpected value for param `option`: {option}')

    with pytest.raises(DuplicateLabelsException) as excinfo:
        with engine.connect() as connection:
            PandasSpecialEngine(connection=connection, table_name=TableNames.NO_TABLE, df=df)
    assert "Found duplicates across index and columns" in str(excinfo.value)


def test_non_unique_index(_):
    engine = create_engine('sqlite://')
    df = pd.DataFrame(index=pd.Index(data=[0, 0], name='ix'))
    with pytest.raises(DuplicateValuesInIndexException) as excinfo:
        with engine.connect() as connection:
            PandasSpecialEngine(connection=connection, table_name=TableNames.NO_TABLE, df=df)
    assert "The index must be unique" in str(excinfo.value)


@pytest.mark.parametrize("bad_chunksize_value", [0, -1, 1.2])
def test_bad_chunksize(_, bad_chunksize_value):
    engine = create_engine('sqlite://')
    df = pd.DataFrame({'test': [0]})
    df.index.name = 'id'
    with engine.connect() as connection:
        pse = PandasSpecialEngine(connection=connection, table_name=TableNames.NO_TABLE, df=df)
        with pytest.raises(ValueError) as excinfo:
            pse._create_chunks(values=[0], chunksize=bad_chunksize_value)
        assert "integer strictly above 0" in str(excinfo.value)
