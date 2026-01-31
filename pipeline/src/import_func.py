#!/usr/bin/env python3

'''Data Import Functions'''
import os
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import mysql.connector
from mysql.connector import Error
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

    def load_db(self, config_db, mode, timekey, yyyymm, data):
        '''
        Function that loads data from source to database table

        Parameters
        ----------
            config_db: config for database
            mode: data load mode - append, truncate, refresh
            timekey: table column used for partition by timekey
            yyyymm: year_mon of data to delete, yyyymm format
            data: data to be inserted organized into a list of tuples
        '''

        connection = self.create_connection()
        tbl = f'{self.database}.{config_db["tbl"]}'

        # perform deletion based on the mode (truncate / refresh)
        if mode == 'truncate':
            del_stmt = f'TRUNCATE {tbl};'
            try:
                cursor = connection.cursor()
                cursor.execute(del_stmt)
                logger.info(f"""TRUNCATE statement executed successfully for {tbl}."""
                            )
            except Error as e:
                logger.error(f'The error {e} occurred.')
                raise
        elif mode == 'refresh':
            curr_yearmon = dt.strptime(yyyymm, '%Y%m')
            next_yearmon = curr_yearmon + relativedelta(months=1)
            sql_curr_yearmon = dt.strftime(curr_yearmon, '%Y-%m-%d')
            sql_next_yearmon = dt.strftime(next_yearmon, '%Y-%m-%d')
            del_stmt = f"""
            DELETE FROM {tbl}
            WHERE `{timekey}` >= '{sql_curr_yearmon}'
            AND `{timekey}` < '{sql_next_yearmon}';
            """.strip()
            try:
                cursor = connection.cursor()
                cursor.execute(del_stmt)
                connection.commit()
                logger.info(f'''DELETE statement executed successfully for {tbl} by timekey = "{sql_curr_yearmon}".''')
            except Error as e:
                logger.error(f'The error {e} occurred.')

        elif mode == 'append':
            pass

        # create the insert statement
        col_name_list = config_db['tbl_col'].values()
        tbl_col_names = ', '.join(f'`{i}`' for i in col_name_list)
        placeholders = ', '.join(['%s'] * len(col_name_list))
        stmt = f'INSERT INTO {tbl} ({tbl_col_names}) VALUES ({placeholders});'

        try:
            cursor = connection.cursor()
            cursor.executemany(stmt, data)
            connection.commit()
            logger.info(f'INSERT statement executed successfully for {tbl}.')
            connection.close()
        except Error as e:
            logger.error(f'The error {e} occurred.')
            raise


if __name__ == '__main__':

    sqlpipe = DataPipe(
        hostname=os.environ['DB_HOST'],
        username=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        database=os.environ['DB_NAME']
    )

    # sqlpipe.load_db()
