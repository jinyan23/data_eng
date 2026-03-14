#!/usr/bin/env python3

import os
import requests
import json
import pandas as pd

import util
from util import UDLogger
import util_s3


ud_logger = UDLogger(filename='api.log', name=__name__)
logger = ud_logger.create_logger()
conf = util.load_config('nea_rainfall.yaml')


def api_rainfall():
    '''
    Performs API call to download rainfall from data gov NEA and save the data
    as a csv and upload into s3
    '''
    bucket = os.environ.get('BUCKET')
    response = requests.get(
        conf['config_proc']['url'],
        headers={'X-Api-Key': os.getenv('NEA_KEY')},
    )

    try:
        data = response.json()['data']
    except json.JSONDecodeError as e:
        logger.error(f'Failed to decode JSON response: {e}')
        raise

    # stations metadata
    stations_df = pd.json_normalize(data['stations'])

    # readings (explode the readings list and the inner data list)
    readings_df = (
        pd.json_normalize(data['readings'])
        .explode('data', ignore_index=True)
    )
    readings_df['stationId'] = readings_df['data'].map(lambda x: x['stationId'])
    readings_df['value'] = readings_df['data'].map(lambda x: x['value'])
    readings_df = readings_df.drop(columns=['data'])

    # join readings with station metadata
    df = readings_df.merge(
        stations_df,
        left_on='stationId',
        right_on='id',
        how='left'
    )

    # process timestamp to yyyy-mm-dd hh-mm-ss
    timestamp_str = df['timestamp'].astype(str)
    ts_series = timestamp_str.str.replace(' ', '', regex=False)
    ts = pd.to_datetime(ts_series, errors='raise')
    if ts.dt.tz is not None:
        ts = ts.dt.tz_localize(None)
    df['timestamp'] = ts

    # save data into s3
    timestamp = df['timestamp'].iloc[0].strftime('%Y%m%d_%H%M%S')
    obj_key = f'incoming/rainfall/rainfall_{timestamp}.csv'

    try:
        util_s3.write_csv_s3(df, 
                             f'{obj_key}',
                             sep='|', 
                             index=False)
        logger.info(f'{obj_key} extracted to s3://{bucket}/{obj_key}')
    except Exception as e:
        logger.error(f'The error {e} occurred.')
        raise


if __name__ == '__main__':
    api_rainfall()
