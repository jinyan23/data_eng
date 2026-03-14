#!/usr/bin/env python3

'''Imports rainfall data from csv into mariadb weather.r_rainfall'''

import os
from io import BytesIO
import hashlib
from datetime import datetime as dt

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from util import load_config
from util_logger import UDLogger
from import_func import DataPipe
import util_s3

# create logger
ud_logger = UDLogger(filename='import.log', name=__name__)
logger = ud_logger.create_logger()

yaml_file = load_config('nea_rainfall.yaml')
config_pv = yaml_file['config_proc']
config_db_tbl = yaml_file['config_db_tbl']

REGISTRY_COLUMNS = ['loaded_at', 'file_name', 'checksum', 'status']


def load_registry(client, bucket, key):
    '''
    Load the registry CSV from S3, or return an empty registry DataFrame.
    '''

    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        df = pd.read_csv(BytesIO(body), dtype=str, keep_default_na=False)
    except ClientError as e:
        code = e.response.get('Error', {}).get('Code')
        if code in ('NoSuchKey', '404'):
            df = pd.DataFrame(columns=REGISTRY_COLUMNS)
        else:
            raise

    if df.empty:
        return pd.DataFrame(columns=REGISTRY_COLUMNS)

    # Ensure all expected columns exist
    for col in REGISTRY_COLUMNS:
        if col not in df.columns:
            df[col] = ''
    return df[REGISTRY_COLUMNS]


def list_objects(client, bucket, prefix):
    '''
    List all S3 objects under a prefix, returning (key, last_modified).
    '''

    items = []
    token = None
    while True:
        kwargs = {'Bucket': bucket, 'Prefix': prefix}
        if token:
            kwargs['ContinuationToken'] = token
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get('Contents', []):
            items.append((obj['Key'], obj['LastModified']))
        if not resp.get('IsTruncated'):
            break
        token = resp.get('NextContinuationToken')
    return items


def _append_registry(registry_df, object_key, checksum, status):
    registry_df.loc[len(registry_df)] = [
        dt.now().strftime('%Y-%m-%d %H:%M:%S'),
        object_key,
        checksum,
        status,
    ]
    return registry_df


def _get_object_bytes(client, bucket, object_key):
    obj = client.get_object(Bucket=bucket, Key=object_key)
    return obj['Body'].read()


def _parse_csv(body, config_pv, config_db_tbl):
    df = pd.read_csv(
        BytesIO(body),
        delimiter=config_pv['delimiter'],
        dtype=config_pv['col_pd'],
        keep_default_na=False,
    )

    # align column order to config
    src_cols = list(config_db_tbl['tbl_col'].keys())
    df = df[src_cols]

    # transform
    df = df.replace(r'^\s*$', None, regex=True)
    df = df.astype(object).where(df.notna(), None)
    return df


def _derive_yyyymm(df):
    if 'timestamp' in df.columns and len(df) > 0:
        try:
            ts = dt.strptime(df['timestamp'].iloc[0], '%Y-%m-%d %H:%M:%S')
            return ts.strftime('%Y%m')
        except Exception:
            return dt.strftime(dt.now(), '%Y%m')
    return dt.strftime(dt.now(), '%Y%m')


def _load_to_db(df, config_db_tbl, config_pv, yyyymm):
    data = [tuple(row) for row in df.to_numpy()]
    sqlpipe = DataPipe(
        hostname=os.environ['DB_HOST'],
        username=os.environ['DB_USER'],
        password=os.environ['DB_PASS'],
        database=os.environ['DB_NAME'],
        port=os.environ['DB_PORT'],
    )
    sqlpipe.load_db(
        config_db=config_db_tbl,
        mode=config_pv['mode'],
        timekey=config_pv['timekey'],
        yyyymm=yyyymm,
        data=data,
    )


def _should_skip_file(object_key, registry_key, success_by_file):
    if object_key == registry_key:
        return True
    if object_key in success_by_file:
        return True
    return False


def import_rainfall():
    '''
    Read in csv file and load into mariadb weather database.
    '''

    logger.info(f'Run executing {__name__}')

    bucket = os.environ.get('BUCKET')
    csv_dir = config_pv['csv_prefix']
    csv_name = config_pv['csv_name']

    prefix = f'{csv_dir}/{csv_name}_'

    registry_key = f'{csv_dir}/registry.csv'

    client = boto3.client('s3')
    registry_df = load_registry(client, bucket, registry_key)

    success_by_file = set(
        registry_df.loc[registry_df['status'] == 'success', 'file_name']
    )
    checksum_success = set(
        registry_df.loc[registry_df['status'] == 'success', 'checksum']
    )
    seen_files = set(registry_df['file_name'])

    objects = list_objects(client, bucket, prefix)
    if not objects:
        logger.info(f'No objects found under s3://{bucket}/{prefix}')
        return

    # Process in chronological order
    objects.sort(key=lambda item: item[1])

    for object_key, _ in objects:
        if _should_skip_file(object_key, registry_key, success_by_file):
            continue

        try:
            body = _get_object_bytes(client, bucket, object_key)
            checksum = hashlib.md5(body).hexdigest()

            # If checksum already loaded, just record the file and skip
            if checksum in checksum_success:
                if object_key not in seen_files:
                    registry_df = _append_registry(
                        registry_df,
                        object_key,
                        checksum,
                        'success',
                    )
                    util_s3.write_csv_s3(registry_df, registry_key, index=False)
                continue

            df = _parse_csv(body, config_pv, config_db_tbl)
            yyyymm = _derive_yyyymm(df)
            _load_to_db(df, config_db_tbl, config_pv, yyyymm)

            registry_df = _append_registry(
                registry_df,
                object_key,
                checksum,
                'success',
            )
            util_s3.write_csv_s3(registry_df, registry_key, index=False)

            row_count = df.shape[0]
            logger.info(f'{__name__}: {object_key} completed, {row_count} rows inserted')

        except Exception as e:
            # record failure and re-raise
            registry_df = _append_registry(
                registry_df,
                object_key,
                checksum if 'checksum' in locals() else '',
                'failed',
            )
            util_s3.write_csv_s3(registry_df, registry_key, index=False)
            logger.error(f'The error {e} occurred.')
            raise


if __name__ == '__main__':
    import_rainfall()
