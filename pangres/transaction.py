"""
Tools for handling transactions in pangres
"""
from sqlalchemy.engine.base import Connectable, Connection, Engine, Transaction
from typing import Union


# # Transaction Handler

class TransactionHandler:
    """
    Context manager class for transactions in pangres.

    Workflow (the numbering below can be found in the comments):

    1) create connection (Engine is passed) or use connection (Connection is passed)
    2) create transaction (Engine is passed)
    3) (commit|rollback) if we created a transaction
    4) close connection if we created the connection from the engine

    We do not handle transactions from users. They will have to commit/rollback themselves.
    We also do not create transactions when a Connection is passed so that commit-as-you-go
    workflows are possible (the operations of pangres will behave like basic SQL executions
    in sqlalchemy).

    Parameters
    ----------
    connectable
        If a sqlalchemy Engine is passed we create a connection,
        otherwise we raise an error if `connectable` is not  a sqlalchemy Connection

    Examples
    --------
    >>> import pandas as pd
    >>> from sqlalchemy import create_engine, text
    >>>
    >>> # config
    >>> engine = create_engine('sqlite:///')
    >>> ddl = text('CREATE TABLE test (id INT);')
    >>> insert = text('INSERT INTO test (id) VALUES (1), (2)')
    >>>
    >>> # execute table creation (ddl) and insert
    >>> with TransactionHandler(connectable=engine) as trans:
    ...     result_ddl = trans.connection.execute(ddl)
    ...     result_insert = trans.connection.execute(insert)
    >>>
    >>> # display the table
    >>> with engine.connect() as con:
    ...     df = pd.read_sql('SELECT * FROM test;', con=con)
    ...     print(df.to_markdown())
    |    |   id |
    |---:|-----:|
    |  0 |    1 |
    |  1 |    2 |
    """

    def __init__(self, connectable:Connectable):
        # set attrs
        self.connectable = connectable
        # we will set this when we enter the context
        self.connection:Connection = None
        self.transaction:Union[Transaction, None] = None

    def _close_resources(self):
        # we are nesting try...finally (see __exit__ method) to ensure
        # both resources get closed and we get all tracebacks (we'll see "... another exception occured")
        # idk if this is the best way to do this but my only other idea was raising two errors at once
        # (in case the transaction and/or the connection does not close) which does not seem user-friendly
        try:
            # close transaction if we created one
            if self.transaction is not None:
                self.transaction.close()
        finally:
            # close connection if we created one
            if self.connection is not None and isinstance(self.connectable, Engine):
                self.connection.close()

    def __enter__(self):

        # 1) get(create) connection
        if isinstance(self.connectable, Engine):
            self.connection = self.connectable.connect()
        elif isinstance(self.connectable, Connection):
            self.connection = self.connectable
        else:
            raise TypeError(f'Expected a sqlalchemy connectable object ({Connectable}). '
                            f'Got {type(self.connectable)}')

        # 2) get transaction
        if isinstance(self.connectable, Engine):
            # handle case where for some reason we would not be able to create a transaction
            try:
                self.transaction = self.connection.begin()
            except Exception as e:  # pragma: no cover
                self._close_resources()
                raise e
        return self

    def _rollback_or_commit(self, exception_occured:bool):
        # case where we were inside a transaction from the user
        # the user will have to handle rollback and commit
        if self.transaction is None:
            return

        # case where we created the transaction
        if exception_occured:
            self.transaction.rollback()
        else:
            self.transaction.commit()

    def __exit__(self, ex_type, value, traceback) -> bool:
        # make sure __enter__ was properly executed
        if self.connection is None:  # pragma: no cover
            raise AssertionError('No active connection. Perhaps the context manager was not properly entered?')

        # combine step 3) rollback or commit and 4) closing resources
        exception_occured = ex_type is not None
        try:
            self._rollback_or_commit(exception_occured=exception_occured)
        finally:
            self._close_resources()
        return not exception_occured  # will be reraised if False
