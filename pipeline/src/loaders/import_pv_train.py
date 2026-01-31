#!/usr/bin/env python3

'''Imports pv_train data from csv into mariadb transport.r_pv_train'''

import os

from datetime import datetime as dt
import pandas as pd
from dateutil.relativedelta import relativedelta

from util import load_config
from util import UDLogger
from import_func import DataPipe
from pathlib import Path

# create logger
ud_logger = UDLogger(filename='import.log', name=__name__)
logger = ud_logger.create_logger()

YAML_FILE = 'lta_pv_train.yaml'
config_pv_train = load_config(YAML_FILE)['config_pv_train']
config_db_tbl = load_config(YAML_FILE)['config_db_tbl']

root = Path(__file__).parent.parent.parent


def import_pv_train():
    '''
    Read in csv file and load into mariadb transport database.
    '''
    logger.info(f'Run executing {__name__}')

    # extract
    # determine backfill logic
    backfill = config_pv_train['backfill']
    csv_dir = config_pv_train['csv_prefix']
    csv_name = config_pv_train['csv_name']
    logger.info(f'Run executing for backfill={backfill}')

    if backfill is False:
        yyyymm = dt.strftime(dt.now() - relativedelta(months=1), '%Y%m')
    else:
        yyyymm = config_pv_train['yyyymm']

    try:
        df = pd.read_csv(root / csv_dir / f'{csv_name}_{yyyymm}.csv',
                         delimiter=config_pv_train['delimiter'],
                         dtype=config_pv_train['col_pd'])
    except Exception as e:
        logger.error(f'The error {e} occurred.')
        raise

    # transform
    df.loc[:, 'YEAR_MONTH'] = df['YEAR_MONTH'].apply(lambda x: x + '-01')

    # convert dataframe into a list of tuples for executemany()
    data = [tuple(row) for row in df.to_numpy()]

    # load
    sqlpipe = DataPipe(
        hostname=os.environ['DB_HOST'],
        username=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        database=os.environ['DB_NAME']
    )
    sqlpipe.load_db(config_db=config_db_tbl,
                    mode=config_pv_train['mode'],
                    timekey=config_pv_train['timekey'],
                    yyyymm=yyyymm,
                    data=data)

    row_count = df.shape[0]
    logger.info(f'{__name__}: {yyyymm} completed, {row_count} rows inserted')


if __name__ == '__main__':
    import_pv_train()
