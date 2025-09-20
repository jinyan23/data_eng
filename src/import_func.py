#!/usr/bin/env python3

'''Data Import Functions'''

import mysql.connector
from mysql.connector import Error
from util import load_config, time_now

# everytime the import.py executes, all new csv will be ingested into db
# it also creates a log that tracks which csv if ingested, and row count added

config_db = load_config('config_db.yaml')

class DataPipe:
    '''
    Create class to perform data import into systems
    '''
    def __init__(self, hostname, username, password, database):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.database = database

    def create_connection(self):
        '''
        Initiate SQL connector
        '''
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.hostname,
                user=self.username,
                password=self.password,
                database=self.database
            )
            print(f'{time_now()} Connection to mariadb {self.database} successful.')
        except Error as e:
            print(f'{time_now()} The error {e} occurred.')

        return connection

    def load_db(self, config_db_tbl, data):
        '''
        Function that loads data from source to database table
        
        Parameters
        ----------
            config_db_tbl: config for database
            data: data to be inserted organized into a list of tuples
        '''
        connection = self.create_connection()

        # create the insert statement
        table = f'{config_db_tbl['db']}.{config_db_tbl['tbl']}'
        table_col_names = ', '.join(f'`{i}`' for i in config_db_tbl['tbl_col'].values())
        placeholders = ', '.join(['%s'] * len(config_db_tbl['tbl_col'].values()))
        stmt = f'INSERT INTO {table} ({table_col_names}) VALUES ({placeholders});'

        try:
            cursor = connection.cursor()
            cursor.executemany(stmt, data)
            connection.commit()
            print(f'{time_now()} Insert statement ran successfully for {table}.')
        except Error as e:
            print(f'{time_now()} The error {e} occurred.')



if __name__=='__main__':

    sqlpipe = DataPipe(
        hostname=config_db['hostname'],
        username=config_db['username'],
        password=config_db['password'],
        database='transport'
    )

    # sqlpipe.load_db()
