#!/usr/bin/env python3

'''Imports pv_bus data from csv into mariadb transport.r_pv_bus'''

import os

from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd

from util import load_config
from util import UDLogger
from import_func import DataPipe
import util_s3

# create logger
ud_logger = UDLogger(filename='import.log', name=__name__)
logger = ud_logger.create_logger()

YAML_FILE = 'lta_pv_bus.yaml'
config_pv_bus = load_config(YAML_FILE)['config_pv_bus']
config_db_tbl = load_config(YAML_FILE)['config_db_tbl']

# root = Path(__file__).parent.parent.parent


def import_pv_bus():
    '''
    Read in csv file and load into mariadb transport database.
    '''
    logger.info(f'Run executing {__name__}')

    # extract
    # determine backfill logic
    backfill = config_pv_bus['backfill']
    csv_dir = config_pv_bus['csv_prefix']
    csv_name = config_pv_bus['csv_name']
    logger.info(f'Run executing for backfill={backfill}')

    if backfill is False:
        yyyymm = dt.strftime(dt.now() - relativedelta(months=1), '%Y%m')
    else:
        yyyymm = config_pv_bus['yyyymm']

    try:
        df = util_s3.read_csv_s3(f'{csv_dir}/{csv_name}_{yyyymm}.csv',
                                 delimiter=config_pv_bus['delimiter'],
                                 dtype=config_pv_bus['col_pd'],
                                 keep_default_na=False)
    except Exception as e:
        logger.error(f'The error {e} occurred.')
        raise

    # transform
    df.loc[:, 'YEAR_MONTH'] = df['YEAR_MONTH'].apply(lambda x: x + '-01')

    def convert_na(x):
        if pd.isna(x) or x == '':
            return None
        return x
    df['TIME_PER_HOUR'] = df['TIME_PER_HOUR'].apply(convert_na)

    # convert dataframe into a list of tuples for executemany()

    data = [tuple(row) for row in df.to_numpy()]

    # load
    sqlpipe = DataPipe(
        hostname=os.environ['DB_HOST'],
        username=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        database=os.environ['DB_NAME'],
    )
    sqlpipe.load_db(config_db=config_db_tbl,
                    mode=config_pv_bus['mode'],
                    timekey=config_pv_bus['timekey'],
                    yyyymm=yyyymm,
                    data=data)

    row_count = df.shape[0]
    logger.info(f'{__name__}: {yyyymm} completed, {row_count} rows inserted')


if __name__ == '__main__':
    import_pv_bus()
