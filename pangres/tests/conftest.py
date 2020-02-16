#!/usr/bin/env python
# coding: utf-8
"""
Configuration of the tests of pangres for pytest.

The class TestDB configures the database. It is set
to work with CircleCI.

You may change the connection string and the schema in class TestDB
temporarily for local testing but **be warned**: 
1) All tables in the schema will be wiped!
2) Please use a local database with dummy credentials
   to avoid accidentaly commiting private information!
"""
from sqlalchemy import create_engine

class TestDB:
    connection_string = "postgresql://circleci_user:password@localhost:5432/circleci_test?sslmode=disable"
    schema = 'tests'
    engine = create_engine(connection_string)


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    testdb = TestDB()
    # clean schema
    testdb.engine.execute(f'DROP SCHEMA IF EXISTS {testdb.schema} CASCADE;')
    testdb.engine.execute(f'CREATE SCHEMA {testdb.schema}')
