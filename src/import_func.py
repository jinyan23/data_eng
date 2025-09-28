#!/usr/bin/env python3

'''Data Import Functions'''
import os
import mysql.connector
from mysql.connector import Error
from util import load_config
from util import UDLogger

# everytime the import.py executes, all new csv will be ingested into db
# it also creates a log that tracks which csv if ingested, and row count added

ud_logger = UDLogger(filename='import.log', name=__name__)
logger = ud_logger.create_logger()

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
            logger.info(f'Connection to mariadb {self.database} successful.')
        except Error as e:
            logger.error(f'The error {e} occurred.')
            raise

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
        table = f'{self.database}.{config_db_tbl['tbl']}'
        table_col_names = ', '.join(f'`{i}`' for i in config_db_tbl['tbl_col'].values())
        placeholders = ', '.join(['%s'] * len(config_db_tbl['tbl_col'].values()))
        stmt = f'INSERT INTO {table} ({table_col_names}) VALUES ({placeholders});'

        try:
            cursor = connection.cursor()
            cursor.executemany(stmt, data)
            connection.commit()
            logger.info(f'Insert statement executed successfully for {table}.')
        except Error as e:
            logger.error(f'The error {e} occurred.')
            raise



if __name__=='__main__':

    sqlpipe = DataPipe(
        hostname=os.environ['DB_HOST'],
        username=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        database=os.environ['DB_NAME']
    )

    # sqlpipe.load_db()
