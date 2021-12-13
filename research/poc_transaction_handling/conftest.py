#!/usr/bin/env python
# coding: utf-8
# +
"""
Tests configuration using code copied from pangres
"""
from sqlalchemy import create_engine

from pangres.helpers import _sqla_gt14


# -

# ## Class TestDB

# +
def pytest_addoption(parser):
    parser.addoption('--sqlite_conn', action="store", type=str, default=None)
    parser.addoption('--pg_conn', action="store", type=str, default=None)
    parser.addoption('--mysql_conn', action="store", type=str, default=None)

def pytest_generate_tests(metafunc):
    options = {'sqlite':metafunc.config.option.sqlite_conn,
               'pg':metafunc.config.option.pg_conn,
               'mysql':metafunc.config.option.mysql_conn}
    conn_strings, ids, futures = [], [], []

    for db_type, conn_string in options.items():
        if conn_string is None:
            continue
        conn_strings.append(conn_string)
        ids.append(create_engine(conn_string).url.drivername)
        futures.append(False)

    # duplicate with future=True if sqlalchemy >= 1.4
    if _sqla_gt14():
        futures.extend([True for i in range(len(ids))])
        ids.extend([id_+'|future' for id_ in ids])
        conn_strings*=2

    assert len(conn_strings) == len(ids) == len(futures)
    if len(conn_strings) == 0:
        raise ValueError('You must provide at least one connection string (e.g. argument --sqlite_conn)!')
    metafunc.parametrize("conn_string, future", list(zip(conn_strings, futures)), ids=ids, scope='module')
