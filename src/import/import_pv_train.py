#!/usr/bin/env python3

'''Imports pv_train data from csv into mariadb transport.r_pv_train'''

from datetime import datetime as dt
import pandas as pd
from dateutil.relativedelta import relativedelta

from util import load_config, time_now
from import_func import DataPipe

YAML_FILE = 'lta_pv_train.yaml'

config_db = load_config('config_db.yaml')
config_pv_train = load_config(YAML_FILE)['config_pv_train']
config_db_tbl = load_config(YAML_FILE)['config_db_tbl']

def import_pv_train():
    '''
    Read in csv file and load into mariadb transport database.
    '''

    backfill = config_pv_train['backfill']
    file_prefix = config_pv_train['file_prefix']

    if backfill is False:
        yyyy_mm = dt.strftime(dt.now().date() - relativedelta(months=1), '%Y%m')
        file_path = f'{file_prefix}_{yyyy_mm}.csv'
    else:
        yyyy_mm = config_pv_train['yyyymm']
        file_path = f'{file_prefix}_{yyyy_mm}.csv'

    df = pd.read_csv(file_path,
                     delimiter=config_pv_train['delimiter'],
                     dtype=config_pv_train['col_pd'])
    df.loc[:, 'YEAR_MONTH'] = df['YEAR_MONTH'].apply(lambda x: x + '-01')

    # convert dataframe into a list of tuples for executemany()
    data = [tuple(row) for row in df.to_numpy()]

    # load into database
    sqlpipe = DataPipe(
        hostname=config_db['hostname'],
        username=config_db['username'],
        password=config_db['password'],
        database=config_db_tbl['db']
    )
    sqlpipe.load_db(config_db_tbl, data)


    print(f'{time_now()} Script import_pv_train completed.')

import_pv_train()
