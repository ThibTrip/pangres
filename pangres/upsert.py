"""
Functions for preparing/compiling and executing upsert statements
in different SQL flavors.
"""
from copy import deepcopy
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.mysql.dml import insert as mysql_insert

# # Main class `UpsertQuery`

class UpsertQuery:
    """
    Helper for creating and executing UPSERT queries in various SQL flavors

    Examples
    --------
    >>> from pangres.helpers import PandasSpecialEngine
    >>> from pangres.examples import DocsExampleTable
    >>> from sqlalchemy import create_engine
    >>> 
    >>> engine = create_engine('sqlite:///:memory:')
    >>> table = PandasSpecialEngine(engine=engine, df=DocsExampleTable.df_upsert, table_name='doc_upsert').table
    >>> upq = UpsertQuery(engine=engine, table=table)
    """
    def __init__(self, engine, table):
        self.engine = engine
        self.table = table

    def _create_pg_query(self, values, if_row_exists):
        insert_stmt = pg_insert(self.table).values(values)
        if if_row_exists == 'update':
            update_cols = [c.name
                           for c in self.table.c
                           if c not in list(self.table.primary_key.columns)]
            # case when there is only an index in the DataFrame i.e. no columns to update
            if len(update_cols) == 0:
                if_row_exists = 'ignore'
            else:
                upsert = insert_stmt.on_conflict_do_update(index_elements=self.table.primary_key.columns,
                                                           set_={k:insert_stmt.excluded[k] for k in update_cols})
        if if_row_exists == 'ignore':
            upsert = insert_stmt.on_conflict_do_nothing()
        return upsert

    def _create_mysql_query(self, values, if_row_exists):
        insert_stmt = mysql_insert(self.table).values(values)
        if if_row_exists == 'update':
            # thanks to: https://stackoverflow.com/a/58180407/10551772
            # prepare kwargs for on_duplicated_key_update (with kwargs and getattr
            # even "bad" column names will resolve e.g. columns with spaces)
            update_cols = {}
            for col in insert_stmt.table.columns:
                col_name = col.name
                if col_name not in self.table.primary_key:
                    update_cols.update({col_name:insert_stmt.inserted[col_name]})
            # case when there is only an index in the DataFrame i.e. no columns to update
            if len(update_cols) == 0:
                if_row_exists = 'ignore'
            else:
                upsert = insert_stmt.on_duplicate_key_update(**update_cols)
        if if_row_exists == 'ignore':
            # thanks to: https://stackoverflow.com/a/50870348/10551772
            upsert = insert_stmt.prefix_with('IGNORE')
        return upsert

    def _create_sqlite_query(self, values, if_row_exists):
        def escape_col(col):
            # unbound column from its table
            # otherwise the column would compile as "table.col_name"
            # which we could not use in e.g. SQlite
            unbound_col = deepcopy(col)
            unbound_col.table = None
            return str(unbound_col.compile(dialect=self.engine.dialect))

        # prepare start of upsert (INSERT VALUES (...) ON CONFLICT)
        upsert = SQLCompiler(dialect=self.engine.dialect,
                             statement=self.table.insert().values(values))

        # append on conflict clause
        pk = [escape_col(c) for c in self.table.primary_key]
        non_pks = [escape_col(c) for c in self.table.columns if c not in list(self.table.primary_key)]
        ondup = f'ON CONFLICT ({",".join(pk)})'
        # always use "DO NOTHING" if there are no primary keys
        if (not non_pks) or (if_row_exists == 'ignore'):
            ondup_action = 'DO NOTHING'
            upsert.string = ' '.join((upsert.string, ondup, ondup_action))
        elif if_row_exists == 'update':
            ondup_action = 'DO UPDATE SET'
            updates = ', '.join(f'{c}=EXCLUDED.{c}' for c in non_pks)
            upsert.string = ' '.join((upsert.string, ondup, ondup_action, updates))
        return upsert

    def create_query(self, db_type, values, if_row_exists):
        r"""
        Helper for creating UPSERT queries in various SQL flavors

        Parameters
        ----------
        db_type : str
            One of "postgres", "mysql", "sqlite" or "other"
        values : list
            The structure for the values must match the table defined
            at instantiation (e.g. same order for the columns)
        if_row_exists : {'ignore', 'update'}

        Examples
        --------
        >>> import pandas as pd
        >>> from pangres.helpers import PandasSpecialEngine
        >>> from pangres.examples import DocsExampleTable
        >>> from sqlalchemy import create_engine
        >>>
        >>> test_values = [['foo', 'foo@test.com', pd.Timestamp('2021-01-01', tz='UTC'), 1.4, True, ['blue']]]
        >>> pprint_query = lambda query: print(str(query).replace('ON CONFLICT', '\nON CONFLICT')
        ...                                    .replace('SET', '\nSET').replace('ON DUPLICATE', '\nON DUPLICATE'))
        >>>
        >>> # sqlite
        >>> engine = create_engine('sqlite:///:memory:')
        >>> table = PandasSpecialEngine(engine=engine, df=DocsExampleTable.df_upsert, table_name='doc_upsert').table
        >>> upq = UpsertQuery(engine=engine, table=table)
        >>> query = upq.create_query(db_type='sqlite', values=test_values, if_row_exists='update')
        >>> pprint_query(query)
        INSERT INTO doc_upsert (ix, email, ts, float, bool, json) VALUES (?, ?, ?, ?, ?, ?) 
        ON CONFLICT (ix) DO UPDATE 
        SET email=EXCLUDED.email, ts=EXCLUDED.ts, float=EXCLUDED.float, bool=EXCLUDED.bool, json=EXCLUDED.json

        >>> # postgres (note: the connection does not need to be valid)
        >>> engine = create_engine('postgresql+psycopg2://user:password@localhost:5432/postgres')
        >>> table = PandasSpecialEngine(engine=engine, df=DocsExampleTable.df_upsert, table_name='doc_upsert').table
        >>> upq = UpsertQuery(engine=engine, table=table)
        >>> query = upq.create_query(db_type='postgres',values=test_values, if_row_exists='update')
        >>> pprint_query(query)
        INSERT INTO public.doc_upsert (ix, email, ts, float, bool, json) VALUES (%(ix_m0)s, %(email_m0)s, %(ts_m0)s, %(float_m0)s, %(bool_m0)s, %(json_m0)s) 
        ON CONFLICT (ix) DO UPDATE 
        SET email = excluded.email, ts = excluded.ts, float = excluded.float, bool = excluded.bool, json = excluded.json

        >>> # mysql (note: the connection does not need to be valid)
        >>> engine = create_engine('mysql+pymysql://user:password@host:3306/db')
        >>> table = PandasSpecialEngine(engine=engine, df=DocsExampleTable.df_upsert, table_name='doc_upsert').table
        >>> upq = UpsertQuery(engine=engine, table=table)
        >>> query = upq.create_query(db_type='mysql',values=test_values, if_row_exists='update')
        >>> pprint_query(query)
        INSERT INTO doc_upsert (ix, email, ts, `float`, bool, json) VALUES (%(ix_m0)s, %(email_m0)s, %(ts_m0)s, %(float_m0)s, %(bool_m0)s, %(json_m0)s) 
        ON DUPLICATE KEY UPDATE email = VALUES(email), ts = VALUES(ts), `float` = VALUES(`float`), bool = VALUES(bool), json = VALUES(json)

        >>> # unsupported databases will raise a NotImplementedError
        >>> try:
        ...     query = upq.create_query(db_type='oracle',values=test_values, if_row_exists='update')
        ... except Exception as e:
        ...     print(e)
        No query creation method for oracle. Expected one of ['postgres', 'mysql', 'sqlite', 'other']
        """
        query_creation_methods = {"postgres":self._create_pg_query,
                                  "mysql":self._create_mysql_query,
                                  "sqlite":self._create_sqlite_query,
                                  "other":self._create_sqlite_query}
        try:
            return query_creation_methods[db_type](values=values, if_row_exists=if_row_exists)
        except KeyError:
            raise NotImplementedError(f'No query creation method for {db_type}. '
                                      f'Expected one of {list(query_creation_methods.keys())}')

    def execute(self, db_type, values, if_row_exists):
        query = self.create_query(db_type=db_type, values=values, if_row_exists=if_row_exists)
        with self.engine.connect() as connection:
            result = connection.execute(query)
            if hasattr(connection, 'commit'):
                connection.commit()
        return result
