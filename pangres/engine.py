# +
"""
Functions/classes/variables for interacting between a pandas DataFrame
and postgres/mysql/sqlite (and potentially other databases).
"""
import pandas as pd
import logging
import re
import sqlalchemy as sa
from copy import deepcopy
from sqlalchemy import JSON, MetaData, select
from sqlalchemy.engine.base import Connection, Engine
from sqlalchemy.sql import null
from sqlalchemy.schema import PrimaryKeyConstraint, CreateSchema, Table
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations
from typing import List, Optional, Union

# local imports
from pangres.helpers import _sqla_gt14
from pangres.logger import log
from pangres.exceptions import (BadColumnNamesException,
                                DuplicateLabelsException,
                                DuplicateValuesInIndexException,
                                HasNoSchemaSystemException,
                                MissingIndexLevelInSqlException,
                                UnnamedIndexLevelsException)
from pangres.upsert_query import UpsertQuery
# -

# # Local helpers

# ## Regexes

# compile some regexes
# column names that will cause issues with psycopg2 default parameter style
# (so we will need to switch to format style when we see such columns)
RE_BAD_COL_NAME = re.compile(r'[\(\)\%]')
# e.g. match "(50)" in "VARCHAR(50)"
RE_CHARCOUNT_COL_TYPE = re.compile(r'(?<=.)+\(\d+\)')
RE_POSTGRES = re.compile(r'psycopg|postgres')


# # Class PandasSpecialEngine

class PandasSpecialEngine:

    def __init__(self,
                 connection:Connection,
                 df:pd.DataFrame,
                 table_name:str,
                 schema:Optional[str]=None,
                 dtype:Optional[dict]=None):
        """
        Interacts with SQL tables via pandas and SQLalchemy table models.

        Attributes
        ----------
        connection : sqlalchemy.engine.base.Connection
            Connection provided during class instantiation
        df : pd.DataFrame
            DataFrame provided during class instantiation
        table_name : str
            Table name provided during class instantiation
        schema : str or None
            SQL schema provided during class instantiation
        table : sqlalchemy.sql.schema.Table
            Sqlalchemy table model for df

        Parameters
        ----------
        connection
            A connection that was for example directly created from a sqlalchemy
            engine (see https://docs.sqlalchemy.org/en/13/core/engines.html
            and examples below) or from pangres's transaction handler class
            (pangres.transaction.TransactionHandler)
        df
            A pandas DataFrame
        table_name
            Name of the SQL table
        schema
            Name of the schema that contains/will contain the table
            For postgres defaults to "public" if not provided.
        dtype : None or dict {str:SQL_TYPE}, default None
            Similar to pd.to_sql dtype argument.
            This is especially useful for MySQL where the length of
            primary keys with text has to be provided (see Examples)

        Examples
        --------
        >>> from sqlalchemy import create_engine
        >>>
        >>> engine = create_engine("sqlite://")
        >>> df = pd.DataFrame({'name':['Albert', 'Toto'],
        ...                    'profileid':[10, 11]}).set_index('profileid')
        >>>
        >>> with engine.connect() as connection: # doctest: +SKIP
        ...     pse = PandasSpecialEngine(connection=connection, df=df, table_name='example')
        ...     print(pse)
        PandasSpecialEngine (id 123456, hexid 0x123456)
        * connection: <sqlalchemy.engine.base.Connection...>
        * schema: None
        * table: example
        * SQLalchemy table model:
        Table('example', MetaData(bind=<sqlalchemy.engine.base.Connection...>),
              Column('profileid', BigInteger(), table=<example>, primary_key=True, nullable=False),
              Column('name', Text(), table=<example>), schema=None)
        * df.head():
        |   profileid | name   |
        |------------:|:-------|
        |          10 | Albert |
        |          11 | Toto   |
        """
        self._db_type = self._detect_db_type(connection)
        if self._db_type == "postgres":
            schema = 'public' if schema is None else schema
            # raise if we find columns with "(", ")" or "%"
            bad_col_names = [col for col in df.columns if RE_BAD_COL_NAME.search(col)]
            if len(bad_col_names) > 0:
                err = ("psycopg2 (Python postgres driver) does not seem to support "
                       "column names with '%', '(' or ')' "
                       "(see https://github.com/psycopg/psycopg2/issues/167). You need to fix "
                       f"these names: {bad_col_names}")
                raise BadColumnNamesException(err)

        # VERIFY ARGUMENTS
        # all index levels have names
        index_names = list(df.index.names)
        if any(ix_name is None for ix_name in index_names):
            raise UnnamedIndexLevelsException("All index levels must be named!")

        # index is unique
        if not df.index.is_unique:
            err = ("The index must be unique since it is used "
                   "as primary key.\n"
                   "Check duplicates using this code (assuming df "
                   " is the DataFrame you want to upsert):\n"
                   ">>> df.index[df.index.duplicated(keep=False)]")
            raise DuplicateValuesInIndexException(err)

        # there are no duplicated names
        fields = list(df.index.names) + df.columns.tolist()
        if len(set(fields)) != len(fields):
            duplicated_labels = [c for c in fields if fields.count(c) > 1]
            raise DuplicateLabelsException("Found duplicates across index "
                                           f"and columns: {duplicated_labels}")

        # detect json columns
        def is_json(col:str):
            s = df[col].dropna()
            return (not s.empty and
                    s.map(lambda x: isinstance(x, (list, dict))).all())
        json_cols = [col for col in df.columns if is_json(col)]

        # merge with dtype from user
        new_dtype = {c:JSON for c in json_cols}
        if dtype is not None:
            new_dtype.update(dtype)
        new_dtype = None if new_dtype == {} else new_dtype

        # create sqlalchemy table model via pandas
        pandas_sql_engine = pd.io.sql.SQLDatabase(engine=connection, schema=schema)
        table = pd.io.sql.SQLTable(name=table_name,
                                   pandas_sql_engine=pandas_sql_engine,
                                   frame=df,
                                   dtype=new_dtype).table

        # change bindings of table (we want a sqlalchemy engine
        # not a pandas_sql_engine)
        metadata = MetaData(bind=connection)
        table.metadata = metadata

        # add PK
        constraint = PrimaryKeyConstraint(*[table.columns[name]
                                            for name in df.index.names])
        table.append_constraint(constraint)

        # add remaining attributes
        self.connection = connection
        self.df = df
        self.schema = schema
        self.table = table

    @staticmethod
    def _detect_db_type(connectable:Union[Connection, Engine]) -> str:
        """
        Identifies whether the dialect of given sqlalchemy
        connection corresponds to postgres, mysql or another sql type.

        Returns
        -------
        sql_type : {'postgres', 'mysql', 'sqlite', 'other'}
        """
        dialect = connectable.dialect.dialect_description
        if RE_POSTGRES.search(dialect):
            return "postgres"
        elif 'mysql' in dialect:
            return "mysql"
        elif 'sqlite' in dialect:
            return 'sqlite'
        else:
            return "other"

    def _raise_no_schema_feature(self):
        """
        Function to raise an error if we try to do operations on schemas on a database that does not
        support such feature.
        """
        # Should I just do self.db_type != 'postgres'? (not sure if any other DBs use schemas)
        if self._db_type not in ('postgres', 'other'):
            raise HasNoSchemaSystemException('Cannot create schemas for given SQL flavor '
                                             '(AFAIK only PostgreSQL has this feature)')

    def schema_exists(self) -> bool:
        self._raise_no_schema_feature()
        if _sqla_gt14():
            insp = sa.inspect(self.connection)
            return self.schema in insp.get_schema_names()
        else:
            return self.connection.dialect.has_schema(self.connection, self.schema)

    def table_exists(self) -> bool:
        """
        Returns True if the table defined in given instance
        of PandasSpecialEngine exists else returns False.
        """
        insp = sa.inspect(self.connection)
        if _sqla_gt14():
            return insp.has_table(schema=self.schema, table_name=self.table.name)
        else:
            # this is not particularly efficient but AFAIK it's the best we can do at connection level
            return self.table.name in insp.get_table_names(schema=self.schema)

    def create_schema_if_not_exists(self):
        """
        Creates the schema defined in given instance of
        PandasSpecialEngine if it does not exist.
        """
        self._raise_no_schema_feature()
        if self.schema is None:
            raise AssertionError('Cannot create schema because it is None. '
                                 'If you used PostgreSQL the schema should have defaulted to `public`. '
                                 'If using something other than PostgreSQL make sure it supports schemas '
                                 '(AFAIK only PostgreSQL has this feature) and retry your operation '
                                 'after setting the default schema `public` yourself '
                                 '(if that is the schema you wish to use).')  # pragma: no cover
        if not self.schema_exists():
            self.connection.execute(CreateSchema(self.schema))

    def create_table_if_not_exists(self):
        """
        Creates the table generated in given instance of
        PandasSpecialEngine if it does not exist.
        """
        self.table.create(checkfirst=True)

    def get_db_columns_names(self) -> List[str]:
        """
        Gets the column names of the SQL table defined
        in given instance of PandasSpecialEngine.
        """
        if _sqla_gt14():
            insp = sa.inspect(self.connection)
            columns_info = insp.get_columns(schema=self.schema, table_name=self.table.name)
        else:
            columns_info = self.connection.dialect.get_columns(connection=self.connection,
                                                               schema=self.schema,
                                                               table_name=self.table.name)
        db_columns_names = [col_info["name"] for col_info in columns_info]
        # handle case of SQlite where no errors are raised in case of a missing table
        # but instead 0 columns are returned by sqlalchemy
        assert len(db_columns_names) > 0
        return db_columns_names

    def add_new_columns(self):
        """
        Adds columns present in df but not in the SQL table
        for given instance of PandasSpecialEngine.

        Notes
        -----
        Sadly, it seems that we cannot create JSON columns.
        """
        # get column names in db
        db_columns = self.get_db_columns_names()
        # create deepcopies of the column because we are going to unbound
        # them for the table model (otherwise alembic would think we add
        # a column that already exists in the database)
        cols_to_add = [deepcopy(col) for col in self.table.columns if col.name not in db_columns]
        # check columns are not index levels
        if any((c.name in self.df.index.names for c in cols_to_add)):
            raise MissingIndexLevelInSqlException('Cannot add any column that is part of the df index!\n'
                                                  "You'll have to update your table primary key or change your "
                                                  "df index")

        ctx = MigrationContext.configure(self.connection)
        op = Operations(ctx)
        for col in cols_to_add:
            col.table = None  # Important! unbound column from table
            op.add_column(self.table.name, col, schema=self.schema)
            log(f"Added column {col} (type: {col.type}) in table {self.table.name} "
                f'(schema="{self.schema}")')

    def get_db_table_schema(self) -> Table:
        """
        Gets the sqlalchemy table model for the SQL table
        defined in given PandasSpecialEngine (using schema and
        table_name attributes to find the table in the database).
        """
        table_name = self.table.name
        schema = self.schema
        connection = self.connection

        metadata = MetaData(bind=connection, schema=schema)
        metadata.reflect(bind=connection, schema=schema, only=[table_name])
        namespace = table_name if schema is None else f'{schema}.{table_name}'
        db_table = metadata.tables[namespace]
        return db_table

    def get_empty_columns(self) -> list:
        """
        Gets a list of the columns that contain no data
        in the SQL table defined in given instance of
        PandasSpecialEngine.
        Uses method get_db_table_schema (see its docstring).

        Returns
        -------
        list of str
            List of names of columns that contain no data (all rows are NULL)
        """
        db_table = self.get_db_table_schema()
        empty_columns = []
        for col in db_table.columns:
            stmt = select(from_obj=db_table,
                          columns=[col],
                          whereclause=col.isnot(None)).limit(1)
            results = self.connection.execute(stmt).fetchall()
            if results == []:
                empty_columns.append(col)
        return empty_columns

    def adapt_dtype_of_empty_db_columns(self):
        """
        Changes the data types of empty columns in the SQL table defined
        in given instance of a PandasSpecialEngine.

        This should only happen in case of data type mismatches.
        This means with columns for which the sqlalchemy table
        model for df and the model for the SQL table have different data types.
        """
        empty_db_columns = self.get_empty_columns()
        db_table = self.get_db_table_schema()
        # if column does not have value in db and there are values
        # in the frame then change the column type if needed
        for col in empty_db_columns:
            # check if the column also exists in df
            if col.name not in self.df.columns:
                continue
            # check same type
            orig_type = db_table.columns[col.name].type.compile(self.connection.dialect)
            dest_type = self.table.columns[col.name].type.compile(self.connection.dialect)
            # remove character count e.g. "VARCHAR(50)" -> "VARCHAR"
            orig_type = RE_CHARCOUNT_COL_TYPE.sub('', orig_type)
            dest_type = RE_CHARCOUNT_COL_TYPE.sub('', dest_type)
            # if same type or we want to insert TEXT instead of JSON continue
            # (JSON is not supported on some DBs so it's normal to have TEXT instead)
            if ((orig_type == dest_type) or
                ((orig_type == 'JSON') and (dest_type == 'TEXT'))):
                continue
            # grab the col/index from the df
            # so we can check if there are any values
            if col.name in self.df.index.names:
                df_col = self.df.index.get_level_values(col.name)
            else:
                df_col = self.df[col.name]
            if df_col.notna().any():
                # raise error if we have to modify the dtype but we have a SQlite engine
                # (SQLite does not support data type alteration)
                if self._db_type == 'sqlite':
                    raise ValueError('SQlite does not support column data type alteration!')
                ctx = MigrationContext.configure(self.connection)
                op = Operations(ctx)
                new_col = self.table.columns[col.name]
                # check if postgres (in which case we have to use "using" syntax
                # to alter columns data types)
                if self._db_type == 'postgres':
                    escaped_col = str(new_col.compile(dialect=self.connection.dialect))
                    compiled_type = new_col.type.compile(dialect=self.connection.dialect)
                    alter_kwargs = {'postgresql_using':f'{escaped_col}::{compiled_type}'}
                else:
                    alter_kwargs = {}
                op.alter_column(table_name=self.table.name,
                                column_name=new_col.name,
                                type_=new_col.type,
                                schema=self.schema,
                                **alter_kwargs)
                log(f"Changed type of column {new_col.name} "
                    f"from {col.type} to {new_col.type} "
                    f'in table {self.table.name} (schema="{self.schema}")')

    @staticmethod
    def _create_chunks(values:list, chunksize:int=10000):
        """
        Chunks a list into a list of lists of size
        :chunksize:.

        Parameters
        ----------
        chunksize : int > 0, default 10000
            Number of values to be inserted at once,
            an integer strictly above zero.
        """
        if not isinstance(chunksize, int) or chunksize <= 0:
            raise ValueError('chunksize must be an integer strictly above 0')
        chunks = [values[i:i + chunksize] for i in range(0, len(values), chunksize)]
        return chunks

    def _get_values_to_insert(self):
        """
        Gets the values to be inserted from the pandas DataFrame 
        defined in given instance of PandasSpecialEngine
        to the coresponding SQL table.

        Returns
        -------
        values : list
            Values from the df attribute that may have been converted
            for SQL compability e.g. pd.Timestamp will be converted
            to datetime.datetime objects.
        """
        # this seems to be the most reliable way to unpack
        # the DataFrame. For instance using df.to_dict(orient='records')
        # can introduce types such as numpy integer which we'd have to deal with
        values = self.df.reset_index().values.tolist()
        for i in range(len(values)):
            row = values[i]
            for j in range(len(row)):
                val = row[j]
                # replace pd.Timestamp with datetime.datetime
                if isinstance(val, pd.Timestamp):
                    values[i][j] = val.to_pydatetime()
                # check if na unless it is list like
                elif not pd.api.types.is_list_like(val) and pd.isna(val):
                    values[i][j] = null()
                # cast pd.Interval to str
                elif isinstance(val, pd.Interval):
                    log('found pd.Interval objects, they will be casted to str',
                        level=logging.WARNING)
                    values[i][j] = str(val)
        return values

    def upsert(self, if_row_exists:str, chunksize:int=10000):
        """
        Generates and executes an upsert (insert update or 
        insert ignore depending on :if_row_exists:) statement
        for given instance of PandasSpecialEngine.

        The values of df will be upserted with different sqlalchemy
        methods depending on the dialect (e.g. using
        sqlalchemy.dialects.postgresql.insert for postgres).
        See more information under pangres.upsert.

        Parameters
        ----------
        if_rows_exists : {'ignore', 'update'}
            If 'ignore' where the primary key matches nothing is
            updated.
            If 'update' where the primary key matches the values
            are updated using what's available in df.
            In both cases rows are inserted for non primary keys.
        chunksize : int > 0, default 900
            Number of values to be inserted at once,
            an integer strictly above zero.
        """
        assert if_row_exists in ('ignore', 'update')
        # convert values if needed
        values = self._get_values_to_insert()
        # create chunks
        chunks = self._create_chunks(values=values, chunksize=chunksize)
        upq = UpsertQuery(connection=self.connection, table=self.table)
        for chunk in chunks:
            upq.execute(db_type=self._db_type, values=chunk, if_row_exists=if_row_exists)

    def upsert_yield(self, if_row_exists:str, chunksize:int=10000):
        """
        Same as method `upsert` but gives back an sqlalchemy object
        (sqlalchemy.engine.cursor.LegacyCursorResult) for each chunk inserted
        with which you can for instance count updated rows.

        Notes
        -----
        During my initial attemps with the new transaction model of pangres
        I had a problem with the connection closing too early when using
        only one methods with a parameter `yield_chunks`.
        This is why I made two separate methods.

        I suppose that this is due to the transaction context manager we use
        in `pangres.executor.Executor`. We were **returning** a generator which most
        likely led to the outer scope regaining control.
        """
        # some unfortunate repetition of method `upsert` (see comments there)
        assert if_row_exists in ('ignore', 'update')
        values = self._get_values_to_insert()
        chunks = self._create_chunks(values=values, chunksize=chunksize)
        upq = UpsertQuery(connection=self.connection, table=self.table)
        # yield chunks
        for chunk in chunks:
            yield upq.execute(db_type=self._db_type, values=chunk, if_row_exists=if_row_exists)

    def __repr__(self):
        text = f"""PandasSpecialEngine (id {id(self)}, hexid {hex(id(self))})
                   * connection: {self.connection}
                   * schema: {self.schema}
                   * table: {self.table.name}
                   * SQLalchemy table model:\n{self.table.__repr__()}"""
        text = '\n'.join([line.strip() for line in text.splitlines()])

        df_repr = (str(self.df.head()) if not hasattr(self.df, 'to_markdown')
                   else str(self.df.head().to_markdown()))
        text += f'\n* df.head():\n{df_repr}'
        return text

