#!/usr/bin/env python3

'''Imports pv_train data from csv into mariadb transport.r_pv_train'''

import os

from datetime import datetime as dt
from dateutil.relativedelta import relativedelta

from util import load_config
from util_logger import UDLogger
from import_func import DataPipe
import util_s3

# create logger
ud_logger = UDLogger(filename='import.log', name=__name__)
logger = ud_logger.create_logger()

YAML_FILE = 'lta_pv_train.yaml'
config_pv = load_config(YAML_FILE)['config_proc']
config_db_tbl = load_config(YAML_FILE)['config_db_tbl']


def import_pv_train():
    '''
    Read in csv file and load into mariadb transport database.
    '''

    logger.info(f'Run executing {__name__}')

    # extract
    # determine backfill logic
    backfill = config_pv['backfill']
    csv_dir = config_pv['csv_prefix']
    csv_name = config_pv['csv_name']
    logger.info(f'Run executing for backfill={backfill}')

    if backfill is False:
        yyyymm = dt.strftime(dt.now() - relativedelta(months=1), '%Y%m')
    else:
        yyyymm = config_pv['yyyymm']

    try:
        df = util_s3.read_csv_s3(f'{csv_dir}/{csv_name}_{yyyymm}.csv',
                                 delimiter=config_pv['delimiter'],
                                 dtype=config_pv['col_pd'],
                                 keep_default_na=False)
    except Exception as e:
        logger.error(f'The error {e} occurred.')
        raise

    # transform
    df.loc[:, 'YEAR_MONTH'] = df['YEAR_MONTH'].apply(lambda x: x + '-01')
    df = df.replace(r'^\s*$', None, regex=True)
    df = df.astype(object).where(df.notna(), None)

    # convert dataframe into a list of tuples for executemany()
    data = [tuple(row) for row in df.to_numpy()]

    # load
    sqlpipe = DataPipe(
        hostname=os.environ['DB_HOST'],
        username=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        database=os.environ['DB_NAME'],
        port=os.environ['DB_PORT']
    )
    sqlpipe.load_db(config_db=config_db_tbl,
                    mode=config_pv['mode'],
                    timekey=config_pv['timekey'],
                    yyyymm=yyyymm,
                    data=data)

    row_count = df.shape[0]
    logger.info(f'{__name__}: {yyyymm} completed, {row_count} rows inserted')


if __name__ == '__main__':
    import_pv_train()
