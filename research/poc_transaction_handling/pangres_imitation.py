import itertools
import pandas as pd
import sys
from contextlib import contextmanager
from loguru import logger  # pip install loguru
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine, Connection, Transaction
from typing import Union

# # Functions that reproduce my library pangres' workflow

# +
df_expected = pd.DataFrame([{'id':0, 'name':'Foo'}, {'id':1, 'name':'Bar'}])

def create_table(connection, table_name):
    ddl = text(f"CREATE TABLE IF NOT EXISTS {table_name} (id BIGINT PRIMARY KEY, name TEXT);")
    connection.execute(ddl)


def generate_upsert_query(table_name, future:bool=False, mysql_like:bool=False) -> (str, dict):
    if mysql_like:
        query = f"""INSERT INTO {table_name} (id, name) VALUES (:id0, :name0), (:id1, :name1)
                    ON DUPLICATE KEY UPDATE id=id, name=name;"""
    else:
        query = f"""INSERT INTO {table_name} (id, name) VALUES (:id0, :name0), (:id1, :name1)
                    ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name;"""
    parameters = {'id0':0, 'name0':'Foo', 'id1':1, 'name1':'Bar'}
    parameters = {'parameters':parameters} if future else parameters
    return query, parameters


def execute_upsert(table_name:str, connection, future:bool=False, mysql_like:bool=False):
    query, parameters = generate_upsert_query(table_name=table_name, future=future, mysql_like=mysql_like)
    for _ in range(10):
        connection.execute(text(query), **parameters)


def execute_upsert_generator(table_name:str, connection, future:bool=False, mysql_like:bool=False):
    for _ in range(10):
        query, parameters = generate_upsert_query(table_name=table_name, future=future, mysql_like=mysql_like)
        yield connection.execute(text(query), **parameters)


# -

# # New upsert function that could handle everything with one connection...
#
# ...and that will also accept connection objects

# ## Context manager helper

class TransactionHandler:
    """
    Context manager class for transactions in pangres.

    Workflow (the numbering below can be found in the comments):

    1) create connection (Engine is passed) or use connection (Connection is passed)
    2) create transaction if not inside one
    3) (commit|rollback) if we created a transaction
    4) close connection if we created the connection from the engine

    Parameters
    ----------
    connectable
        If a sqlalchemy Engine is passed we create a connection,
        otherwise we raise an error if `connectable` is not  a sqlalchemy Connection
    """

    def __init__(self, connectable:Union[Connection, Engine]):
        # set attrs
        self.connectable = connectable
        # we will set this when we enter the context
        self.connection:Connection = None
        self.transaction:Union[Transaction, None] = None

    def __enter__(self):
        logger.debug('Enter')

        # 1) get(create) connection
        if isinstance(self.connectable, Engine):
            logger.debug('Acquiring connection')
            self.connection = self.connectable.connect()
        elif isinstance(self.connectable, Connection):
            logger.debug('A connection was passed')
            self.connection = self.connectable
        else:
            raise TypeError(f'Expected a sqlalchemy connectable object ({Connection} or {Engine}). '
                            f'Got {type(self.connectable)}')

        # 2) get transaction
        if not self.connection.in_transaction():
            self.transaction = self.connection.begin()
        return self

    def rollback_or_commit(self, exception_occured:bool):
        # case where we were inside a transaction from the user
        # the user will have to handle rollback and commit
        if self.transaction is None:
            return

        # case where we created the transaction
        if exception_occured:
            logger.debug('Rolling back')
            self.transaction.rollback()
        else:
            self.transaction.commit()

    def __exit__(self, ex_type, value, traceback):
        logger.debug('Exit')
        # make sure __enter__ was properly executed
        if self.connection is None:
            raise AssertionError('No active connection')

        # combine step 3) rollback or commit and 4) closing resources
        # there is probably a better way than nesting try...finally to ensure
        # both resources get closed and we get all tracebacks (we'll see "... another exception occured")
        # but my only other idea was raising two errors at once (in case the transaction
        # and/or the connection does not close) and it is not great either
        exception_occured = ex_type is not None
        try:
            self.rollback_or_commit(exception_occured=exception_occured)
        finally:
            try:
                # close transaction if we created one
                if self.transaction is not None:
                    self.transaction.close()
            finally:
                # close connection if we created one (user passed an Engine and not a Connection)
                if isinstance(self.connectable, Engine):
                    logger.debug('Closing connection we created at the context manager start')
                    self.connection.close()
        return not exception_occured  # will be reraised if False


# ## Function

# +
class FakeError(Exception):
    pass

class UpsertExecutor:
    def __init__(self, con, table_name:str, future:bool=False, simulate_error:bool=False):
        self.con = con
        self.table_name = table_name
        # for test purposes only
        self.future = future
        self.simulate_error = simulate_error

    def _setup_objects(self, trans):
        # there would be other things here (postgres schema creation, addding columns, ...)
        # but for now let's just create the table
        create_table(connection=trans.connection, table_name=self.table_name)

    def execute_with_generator(self, mysql_like:bool=False):
        with TransactionHandler(connectable=self.con) as trans:
            self._setup_objects(trans=trans)
            for result in execute_upsert_generator(table_name=self.table_name,
                                                   connection=trans.connection,
                                                   mysql_like=mysql_like,
                                                   future=self.future):
                yield result
                # simulate error during generator execution
                if self.simulate_error: raise FakeError('TEST ERROR')

    def execute_without_generator(self, mysql_like:bool=False):
        with TransactionHandler(connectable=self.con) as trans:
            self._setup_objects(trans=trans)
            execute_upsert(table_name=self.table_name, connection=trans.connection, future=self.future, mysql_like=mysql_like)
            if self.simulate_error: raise FakeError('TEST ERROR')


def upsert(con, table_name:str, yield_chunks:bool=False, future:bool=False, simulate_error:bool=False, mysql_like:bool=False):
    executor = UpsertExecutor(con=con, table_name=table_name, future=future, simulate_error=simulate_error)
    if yield_chunks:
        return executor.execute_with_generator(mysql_like=mysql_like)
    else:
        executor.execute_without_generator(mysql_like=mysql_like)
