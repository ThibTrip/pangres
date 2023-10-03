import random
import datetime
import json
import pandas as pd
from sqlalchemy import (Column, BOOLEAN, DATETIME, FLOAT,
                        JSON, TEXT, text, VARCHAR)
from sqlalchemy.engine import Engine
from sqlalchemy.sql.compiler import IdentifierPreparer
from typing import Union
# local imports
from pangres.helpers import _sqla_gt20


# # Tool for generating example tables

# +

if _sqla_gt20():
    from sqlalchemy.orm import declarative_base
else:
    from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class _TestsExampleTable(Base):  # type: ignore  # this is valid
    """
    Example table compatible with Postgres, SQLite and MySQL for testing.
    """
    __tablename__ = 'pangres_example'
    # use VARCHAR for the pk so MySQL doesn't complain...
    # MySQL does not want variable length text as index
    profileid = Column(VARCHAR(10), primary_key=True)
    email = Column(TEXT)
    timestamp = Column(DATETIME(timezone=True))
    size_in_meters = Column(FLOAT)
    likes_pizza = Column(BOOLEAN)
    favorite_colors = Column(JSON)

    @staticmethod
    def create_example_df(nb_rows):
        emails = ['foo', 'bar', 'baz', 'test', 'abc', 'foobar', 'foobaz']
        domains = ['gmail.com', 'yahoo.fr', 'yahoo.com', 'outlook.fr']
        email_choices = []
        for i in range(nb_rows):
            email = random.choice(emails)
            domain = random.choice(domains)
            email_choices.append(f'{email}@{domain}')
        timestamps = [(datetime.datetime
                       .fromtimestamp(random.randint(1_000_000_000, 1_300_000_000))
                       .astimezone(datetime.timezone.utc))
                      for i in range(nb_rows)]
        colors = ['yellow', 'blue', 'pink', 'red', 'orange', 'brown']
        favorite_colors = []
        for i in range(nb_rows):
            row = [random.choice(colors) for i in range(random.randint(1, 3))]
            favorite_colors.append(row)
        data = {'profileid': range(nb_rows),
                'email': email_choices,
                'timestamp': timestamps,
                'size_in_meters': [random.uniform(1.5, 2.3) for i in range(nb_rows)],
                'likes_pizza': [random.choice([True, False]) for i in range(nb_rows)],
                'favorite_colors': favorite_colors}
        df = pd.DataFrame(data).set_index('profileid')
        return df

    @staticmethod
    def _get_table_namespace(con, schema: Union[str, None], table_name: str) -> str:
        """
        Gets quoted table namespace (`schema.table_name`) to protect against
        SQL injection.
        """
        quote_object_name = lambda object_name: IdentifierPreparer(dialect=con.dialect).quote(object_name)
        schema = quote_object_name(object_name=schema) if schema is not None else None
        table_name = quote_object_name(object_name=table_name)
        return f'{schema}.{table_name}' if schema is not None else table_name

    @staticmethod
    def _wrangle_df_from_db(df: pd.DataFrame) -> pd.DataFrame:
        """
        Helper for method `read_from_db`
        """
        # helpers
        # for SQlite we receive strings back (or None) for a JSON column.
        # for Postgres we receive lists or dicts (or None) back.
        load_json_if_needed = lambda obj: json.loads(obj) if isinstance(obj, str) else obj

        return (df.set_index('profileid')
                .astype({'likes_pizza': bool})
                .assign(timestamp=lambda df: pd.to_datetime(df['timestamp'], utc=True))
                .assign(favorite_colors=lambda df: df['favorite_colors'].map(load_json_if_needed)))

    @staticmethod
    def read_from_db(engine: Engine, schema: str, table_name: str) -> pd.DataFrame:
        """
        Read SQL table containing data that was generated
        using method `create_example_df`
        """
        namespace = _TestsExampleTable._get_table_namespace(con=engine, schema=schema,
                                                            table_name=table_name)
        with engine.connect() as connection:
            df_db = pd.read_sql(text(f'SELECT * FROM {namespace}'), con=connection)
            return _TestsExampleTable._wrangle_df_from_db(df=df_db)

    @staticmethod
    async def _fallback_empty_df(df: pd.DataFrame, engine, namespace: str) -> pd.DataFrame:
        """
        Depending on the SQL flavour behind a proxy (for aread_from_db we have to pass a
        proxy as we cannot use pd.read_sql in async mode), pandas may or may not find columns
        and indices if there is no data in the proxy. E.g. for asyncpg this does not seem
        to work but for aiosqlite it seems to work. This is a workaround to retrieve
        the columns, indices and proper datatypes in such a case.
        """
        if all(len(obj) == 0 for obj in (df.index, df.columns, df)):  # type: ignore  # all objects do have a length
            async with engine.connect() as connection:
                get_df = lambda conn: pd.read_sql(text(f'SELECT * FROM {namespace};'), con=conn)
                return await connection.run_sync(get_df)
        else:
            return df

    @staticmethod
    async def aread_from_db(engine, schema: str, table_name: str) -> pd.DataFrame:
        """
        Async variant of `read_from_db`
        """
        namespace = _TestsExampleTable._get_table_namespace(con=engine, schema=schema,
                                                            table_name=table_name)
        async with engine.connect() as connection:
            proxy = await connection.execute(text(f'SELECT * FROM {namespace};'))
            df = pd.DataFrame(proxy)
            df = await _TestsExampleTable._fallback_empty_df(df=df, engine=engine, namespace=namespace)
            return _TestsExampleTable._wrangle_df_from_db(df=df)


# -

# # Static DataFrame examples

class DocsExampleTable:
    """
    Example DataFrames for the docs.
    """
    # create some test data
    _data = {'full_name': ['John Rambo', 'The Rock', 'John Travolta'],
             'likes_sport': [True, True, False],
             'updated': [pd.Timestamp('2020-02-01', tz='UTC'),
                         pd.Timestamp('2020-04-01', tz='UTC'), pd.NaT],
             'size_in_meters': [1.77, 1.96, None]}
    # create DataFrame using this test data
    df = pd.DataFrame(_data).set_index('full_name')
    # create test data for showing an INSERT UPDATE
    _new_data = {'full_name': ['John Travolta', 'Arnold Schwarzenegger'],
                 'likes_sport': [True, True],
                 'updated': [pd.Timestamp('2020-04-04', tz='UTC'), pd.NaT],
                 'size_in_meters': [1.88, 1.88]}
    new_df = pd.DataFrame(_new_data).set_index('full_name')
    # create test data for showing an INSERT IGNORE
    _new_data2 = {'full_name': ['John Travolta', 'John Cena'],
                  'likes_sport': [True, True],
                  'updated': [pd.NaT, pd.NaT],
                  'size_in_meters': [2.50, 1.84]}
    new_df2 = pd.DataFrame(_new_data2).set_index('full_name')

    # test DataFrame for the methods of upsert.UpsertQuery
    df_upsert = pd.DataFrame(index=pd.Index(data=['foo', 'bar', 'baz'], name='ix'))
    df_upsert['email'] = ['abc@outlook.fr', 'baz@yahoo.fr', 'foobar@gmail.com']
    df_upsert['ts'] = [pd.Timestamp('2021-01-01', tz='UTC')] * 3
    df_upsert['float'] = [1.1, 1.2, 1.3]
    df_upsert['bool'] = [True, False, False]
    df_upsert['json'] = [['red', 'yellow'], ['yellow'], ['yellow', 'red']]
