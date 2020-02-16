#!/usr/bin/env python
# coding: utf-8
"""
Functions/classes/variables for interacting between
a pandas DataFrame and postgres.
"""
import pandas as pd
import warnings
import sqlalchemy.dialects.postgresql.base as pg_sql_types
import logging
from inspect import cleandoc
from sqlalchemy import MetaData, Table, select
from sqlalchemy.exc import DataError
from sqlalchemy.schema import PrimaryKeyConstraint, CreateColumn, Column
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.sqltypes import (TEXT, Text, Float, FLOAT, BigInteger,
                                     BIGINT, Integer, INTEGERTYPE, INT, Integer,
                                     TIMESTAMP, BOOLEAN, Boolean, BOOLEANTYPE)
# configure logger
logging_format = ('%(asctime)s | %(levelname)s     '
                  '| pangres     | %(module)s:%(funcName)s:%(lineno)s '
                  '- %(message)s')
logging.basicConfig(format=logging_format, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('pangres')
logger.setLevel(logging.INFO)

# dictionnary for comparing sqlalchemy data types and altering data types in
# databases.
# IMPORTANT NOTE: perhaps not the most elegant solution but it works.
# I may have missed some dtypes though.
# Please tell me if you have a better idea.
# Also not sure if we need uppercase/camelcase variants and what the
# difference is e.g. Text and TEXT.
dtypes_comparator = {
    BigInteger: "integer",
    BIGINT: "integer",
    Integer: "integer",
    INTEGERTYPE: "integer",
    INT: "integer",
    Text: "text",
    TEXT: "text",
    TIMESTAMP: "timestamp",
    Float: "float",
    FLOAT: "float",
    Boolean: "boolean",
    BOOLEAN: "boolean",
    BOOLEANTYPE: "boolean",
    # pg specific types
    pg_sql_types.DOUBLE_PRECISION: "float",
    pg_sql_types.TIMESTAMP: "timestamp",
    pg_sql_types.BOOLEAN: "boolean"
}

# characters that we do not want to see in column names
forbidden_chars = "()%"
_translator_forbidden_chars = {ord(char): "" for char in forbidden_chars}


class PandasSpecialEngine:

    def __init__(self,
                 engine,
                 df,
                 table_name,
                 schema='public',
                 clean_column_names=False):
        """
        Interact with a postgres table via pandas and SQLalchemy table models.

        Parameters
        ----------
        engine : sqlalchemy.engine.base.Engine
            Engine from sqlalchemy (see sqlalchemy.create_engine)
        df : pd.DataFrame
            A pandas DataFrame
        table_name : str
            Name of the postgres table
        schema : str, default 'public'
            Name of the postgres schema that contains the table
        clean_column_names : bool, default False
            If False raises a ValueError if any of the following
            characters are found in the column names: "(", ")" and "%".
            If True removes any of the aforementionned characters
            in the column names before updating the table.
            Our tests seem to indicate those characters can
            cause issues with psycopg2 even in parameterized
            queries (they are not properly escaped).
        
        Examples
        --------
        >>> from sqlalchemy import create_engine
        >>> pse = PandasSpecialEngine(engine=create_engine("postgresql://user:password@host.com:5432/database"), 
                                  df=pd.DataFrame({'name':['Albert'],'profileid':[0]}).set_index('profileid'), 
                                  table_name='example')
        pse
        
        """
        # VERIFY ARGUMENTS
        # all index levels have names
        index_names = list(df.index.names)
        if any([ix_name is None for ix_name in index_names]):
            raise IndexError("All index levels must be named!")

        # index is unique
        if not df.index.is_unique:
            err_msg = ("The index must be unique since it is used "
                       "as primary key.\n"
                       "Check duplicates using this code (assuming df "
                       " is the DataFrame you want to upsert):\n"
                       ">>> df.index[df.index.duplicated(keep=False)]")
            raise IndexError(err_msg)

        # no forbidden characters are contained in the column names
        # tests have shown that even within a parameterized query
        # signs like ")", "(" and "%" may cause errors with psycopg2
        new_df = df.copy()
        for col in new_df.columns:
            new_df.rename(columns={
                col: self._clean_column_name(col, raise_=not clean_column_names)
            },
                          inplace=True)
        # also do this for index names
        new_index_names = []
        for index_name in index_names:
            new_index_name = self._clean_column_name(
                index_name, raise_=not clean_column_names)
            new_index_names.append(new_index_name)
        new_df.rename_axis(new_index_names, axis='index', inplace=True)

        # there are no duplicated names
        # IMPORTANT: verify this after cleaning names
        fields = new_index_names + new_df.columns.tolist()
        if len(set(fields)) != len(fields):
            raise ValueError(("There cannot be duplicated names amongst "
                              "index levels and/or columns!"))

        # create sqlalchemy table model via pandas
        pandas_sql_engine = pd.io.sql.SQLDatabase(engine=engine, schema=schema)
        table = pd.io.sql.SQLTable(name=table_name,
                                   pandas_sql_engine=pandas_sql_engine,
                                   frame=new_df).table

        # change bindings of table (we want a sqlalchemy engine
        # not a pandas_sql_engine)
        metadata = MetaData(bind=engine)
        table.metadata = metadata

        # add PK
        constraint = PrimaryKeyConstraint(
            *[table.columns[name] for name in new_index_names])
        table.append_constraint(constraint)

        # ADD ATTRIBUTES
        self.engine = engine
        self.df = new_df
        self.schema = schema
        self.table_name = table_name
        self.namespace = f'{schema}."{table_name}"'
        # get a list of all fields in the frame (index and columns)
        self.df_col_names = [col_info.name for col_info in table.columns]
        self.table = table

    @staticmethod
    def _clean_column_name(column_name: str, raise_: bool = False) -> str:
        """
        Renames or raises if a column name contains ")", "(" or "%".

        Parameters
        ----------
        column_name : str
            Name of the column/index name to examine/rename
        raise_ : bool, default False
            If True raises an error if a column name
            contains ")", "(" or "%"
            else those characters are removed

        Returns
        -------
        str
            column_name without ")", "(" and "%"
        """
        # ignore integer columns etc
        if not isinstance(column_name, str):
            return column_name

        before = column_name
        column_name = column_name.translate(_translator_forbidden_chars)

        if before != column_name:
            if raise_:
                raise ValueError(
                    (f'The column "{before}" contains at least one '
                     f"of the forbidden characters: {list(forbidden_chars)}"))

            else:
                warnings.warn((f'The column "{before}" has been renamed to '
                               f'"{column_name}"'))

        return column_name

    def table_exists(self) -> bool:
        """
        Returns True if the postgres table defined
        in the instance of a PandasSpecialEngine exists
        else returns False.

        Returns
        -------
        bool
            True if table exists else False
        """
        return self.engine.has_table(self.table_name, schema=self.schema)

    def create_schema_if_not_exists(self):
        """
        Creates the postgres schema defined
        in the instance of a PandasSpecialEngine
        if it does not exist.
        """
        if self.schema is not None:
            self.engine.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")

    def create_table_if_not_exists(self):
        """
        Creates the postgres table defined
        in the instance of a PandasSpecialEngine
        if it does not exist.
        """
        self.table.create(checkfirst=True)

    def get_db_columns_names(self) -> list:
        """
        Gets the column names of the postgres table defined
        in the instance of a PandasSpecialEngine.

        Returns
        -------
        list
            list of column names (str)
        """
        columns_info = self.engine.dialect.get_columns(
            connection=self.engine,
            table_name=self.table_name,
            schema=self.schema)
        db_columns_names = [col_info["name"] for col_info in columns_info]
        return db_columns_names

    def get_db_columns_types(self) -> list:
        """
        Gets the column data types of the postgres table defined
        in the instance of a PandasSpecialEngine.

        The data types are sqlalchemy sql types
        e.g. sqlalchemy.sql.sqltypes.Text.

        Returns
        -------
        list
            list of column types (misc. objects)
        """
        columns_info = self.engine.dialect.get_columns(
            connection=self.engine,
            table_name=self.table_name,
            schema=self.schema)
        db_columns_types = {
            col_info["name"]: col_info["type"] for col_info in columns_info
        }
        return db_columns_types

    def add_new_columns(self):
        """
        Adds columns present in the DataFrame that
        are not in the postgres table defined
        in the instance of a PandasSpecialEngine.
        """
        cols_to_add = [
            col for col in self.df_col_names
            if col not in self.get_db_columns_names()
        ]

        # make ALTER STATEMENTS for each column (it seems it has to be RAW SQL\
        # (see https://stackoverflow.com/questions/8236647/)
        for col in cols_to_add:

            # get str for adding column e.g. "name TEXT"
            escaped_col = str(
                CreateColumn(self.table.columns[col]).compile(self.engine))
            alter_statement = f"""ALTER TABLE {self.namespace}
                                  ADD COLUMN {escaped_col};
                               """

            self.engine.execute(alter_statement)
            logger.info((f"Added column {escaped_col} in {self.namespace}"))

    def _get_db_table_schema(self):
        """
        Gets the sqlalchemy table model of the postgres table
        defined in the instance of a PandasSpecialEngine.
        """
        metadata = MetaData(bind=self.engine, schema=self.schema)
        metadata.reflect(bind=self.engine)
        db_table = Table(self.table_name,
                         metadata,
                         autoload=True,
                         autoload_with=self.engine)
        return db_table

    def _get_empty_columns(self):
        """
        Gets a list of the columns that contain no data
        in the postgres table defined in the instance
        of a PandasSpecialEngine.

        Returns
        -------
        list
            list of column names (str)
        """
        db_table = self._get_db_table_schema()
        db_column_names = self.get_db_columns_names()
        empty_columns = []

        for col in db_column_names:
            not_null = Column(col).isnot(None)
            stmt = select(columns=["*"],
                          from_obj=db_table,
                          whereclause=not_null)
            results = self.engine.execute(stmt.limit(1)).fetchall()
            if results == []:
                empty_columns.append(col)

        return empty_columns

    def adapt_dtype_of_empty_db_columns(self):
        """
        Changes the data types of empty columns in the
        posgres table defined in the instance of a
        PandasSpecialEngine.

        This only happens in case of data type
        mismatches. This means with columns for
        which the DataFrame in the PandasSpecialEngine
        instance has a different data type. This is also
        only applied to columns that are not empty in the
        DataFrame.
        """
        empty_db_columns = self._get_empty_columns()
        db_columns_types = self.get_db_columns_types()

        for col in self.df_col_names:

            if col in self.df.index.names:
                df_col = self.df.index.get_level_values(col)
            else:
                df_col = self.df[col]

            # if column does not have value in db and there are values
            # in the frame
            if col in empty_db_columns and df_col.notna().any():
                dtype_sqla_frame = type(self.table.columns[col].type)
                dtype_sqla_db = type(db_columns_types[col])

                dtype_pg_frame = dtypes_comparator.get(dtype_sqla_frame)
                dtype_pg_db = dtypes_comparator.get(dtype_sqla_db)

                if dtype_pg_frame is None:
                    logger.warning(("encoutered unknown sqlalchemy data type "
                                    f"in frame: {dtype_sqla_frame}"))

                if dtype_pg_db is None:
                    logger.warning(("encoutered unknown sqlalchemy data type"
                                    f"in db: {dtype_sqla_db}"))
                # adapt dtype in db according to frame
                if (dtype_pg_frame is not None and dtype_pg_db is not None and
                        dtype_pg_frame != dtype_pg_db):
                    alter_stmt = f"""ALTER TABLE {self.namespace}
                                     ALTER COLUMN "{col}" TYPE {dtype_pg_frame}
                                         USING "{col}"::{dtype_pg_frame};"""
                    try:
                        self.engine.execute(alter_stmt)
                        logger.info(("Adapted column type in postgres according"
                                     f' to frame, column "{col}" is now'
                                     f' of dtype {dtype_pg_frame}'))
                    except DataError:
                        logger.warning(("Could not adapt column type in "
                                        "postgres according to frame "
                                        f"({dtype_pg_frame})"))

    @staticmethod
    def _split_list_in_chunks(l: list, chunksize: int) -> list:
        """
        Splits a list in chunks of n sized lists.

        The last chunk may have less values.

        Returns
        -------
        list
            list of lists
        """
        if not isinstance(chunksize, int) or chunksize <= 0:
            raise ValueError('chunksize must be an integer strictly above 0')

        return [l[i:i + chunksize] for i in range(0, len(l), chunksize)]

    def _get_values_to_insert(self, chunksize=10000):
        """
        Gets the values to be inserted from the pandas
        DataFrame defined in the instance of a
        PandasSpecialEngine to the coresponding
        postgres table.

        Parameters
        ----------
        chunksize : int, default 10000
            Number of values to be inserted at once,
            an integer strictly above zero.

        Returns
        -------
        list
            list of lists
            each list represents a chunk of size :chunksize:
            to be inserted
        """
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
                    values[i][j] = None

                # cast pd.Interval to str
                elif isinstance(val, pd.Interval):
                    warnings.warn(('found pd.Interval objects, '
                                   'they will be casted to str'))
                    values[i][j] = str(val)

        chunks = self._split_list_in_chunks(values, chunksize=chunksize)
        return chunks

    def insert(self, if_exists, chunksize=10000):
        """
        Inserts values from the pandas
        DataFrame defined in the instance of a
        PandasSpecialEngine to the coresponding
        postgres table.

        Values are inserted either using postgres
        [ON CONFLICT DO UPDATE] mode or
        [ON CONFLICT DO NOTHING] mode.

        Parameters
        ----------
        if_exists : str
            One of 'upsert_overwrite' or 'upsert_keep'
            if 'upsert_overwrite' all the data is updated
            where the primary key matches,
            if 'upsert_keep' all the data is kept
            where the primary key matches.

        chunksize : int, default 10000
            Number of values to be inserted at once,
            an integer strictly above zero.
        """

        chunks = self._get_values_to_insert(chunksize=chunksize)

        for chunk in chunks:
            insert_table = insert(self.table).values(chunk)

            # adapt statement depending on value of "if_exists"
            if if_exists == "upsert_overwrite":

                update_cols = [
                    c.name
                    for c in self.table.c
                    if c not in list(self.table.primary_key.columns)
                ]

                insert_table_sql = insert_table.on_conflict_do_update(
                    index_elements=self.table.primary_key.columns,
                    set_={
                        k: getattr(insert_table.excluded, k)
                        for k in update_cols
                    })

            elif if_exists == "upsert_keep":
                insert_table_sql = insert_table.on_conflict_do_nothing()

            else:
                raise ValueError(('if_exists must be either "upsert_overwrite"'
                                  'or "upsert_keep"'))

            insert_table_sql.execute()

    def __repr__(self):
        text = f"""# PandasSpecialEngine (id {id(self)}, hexid {hex(id(self))})
                   ## connection: {self.engine}
                   ## df.head():\n{self.df.head()}
                   ## schema: "{self.schema}"
                   ## table_name: "{self.table_name}"
                   ## SQLalchemy table model:\n{self.table.__repr__()}"""
        text = '\n'.join([line.strip() for line in text.splitlines()])
        return text
