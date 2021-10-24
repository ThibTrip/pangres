# +
import random
import datetime
import pandas as pd
from sqlalchemy import (Column, TEXT, FLOAT, BOOLEAN,
                        JSON, VARCHAR, DATETIME)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class _TestsExampleTable(Base):
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

    @classmethod
    def create_example_df(cls, nb_rows):
        emails = ['foo', 'bar', 'baz', 'test', 'abc', 'foobar', 'foobaz']
        domains = ['gmail.com', 'yahoo.fr', 'yahoo.com', 'outlook.fr']
        email_choices = []
        for i in range(nb_rows):
            email = random.choice(emails)
            domain = random.choice(domains)
            email_choices.append(f'{email}@{domain}')
        timestamps = [(datetime.datetime
                       .fromtimestamp(random.randint(1000000000,1300000000))
                       .astimezone(datetime.timezone.utc))
                      for i in range(nb_rows)]
        colors = ['yellow', 'blue', 'pink', 'red', 'orange', 'brown']
        favorite_colors = []
        for i in range(nb_rows):
            l = [random.choice(colors) for i in range(random.randint(1,3))]
            favorite_colors.append(l)
        data = {# IMPORTANT! for our tests make profileid incremental!
                # it's not an integer (see table definition for an explanation why not)
                # but we just add a prefix or something to an incremented number
                'profileid':[str(f'abc{i}') for i in range(nb_rows)],
                'email':email_choices,
                'timestamp':timestamps,
                'size_in_meters':[random.uniform(1.5,2.3) for i in range(nb_rows)],
                'likes_pizza':[random.choice([True, False]) for i in range(nb_rows)],
                'favorite_colors':favorite_colors}
        df = pd.DataFrame(data).set_index('profileid')
        return df

class DocsExampleTable():
    """
    Example DataFrames for the docs.
    """
    # create some test data
    _data = {'full_name':['John Rambo', 'The Rock', 'John Travolta'],
             'likes_sport':[True, True, False],
             'updated':[pd.Timestamp('2020-02-01', tz='UTC'),
                        pd.Timestamp('2020-04-01', tz='UTC'), pd.NaT],
             'size_in_meters':[1.77, 1.96, None]}
    # create DataFrame using this test data
    df = pd.DataFrame(_data).set_index('full_name')
    # create test data for showing an INSERT UPDATE
    _new_data = {'full_name':['John Travolta', 'Arnold Schwarzenegger'],
                 'likes_sport':[True, True],
                 'updated':[pd.Timestamp('2020-04-04', tz='UTC'), pd.NaT],
                 'size_in_meters':[1.88, 1.88]}
    new_df =  pd.DataFrame(_new_data).set_index('full_name')
    # create test data for showing an INSERT IGNORE
    _new_data2 = {'full_name':['John Travolta', 'John Cena'],
                  'likes_sport':[True, True],
                  'updated':[pd.NaT, pd.NaT],
                  'size_in_meters':[2.50, 1.84]}
    new_df2 =  pd.DataFrame(_new_data2).set_index('full_name')

    # test DataFrame for the methods of upsert.UpsertQuery
    df_upsert = pd.DataFrame(index=pd.Index(data=['foo', 'bar', 'baz'], name='ix'))
    df_upsert['email'] = ['abc@outlook.fr', 'baz@yahoo.fr', 'foobar@gmail.com']
    df_upsert['ts'] = [pd.Timestamp('2021-01-01', tz='UTC')]*3
    df_upsert['float'] = [1.1, 1.2, 1.3]
    df_upsert['bool'] = [True, False, False]
    df_upsert['json'] = [['red', 'yellow'], ['yellow'], ['yellow', 'red']]    
