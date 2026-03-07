#!/usr/bin/env python3

from datetime import datetime as dt
import os
from io import BytesIO
from pathlib import PurePosixPath
import zipfile

import util
from util import UDLogger
import util_s3
import boto3

ud_logger = UDLogger(filename='api.log', name=__name__)
logger = ud_logger.create_logger()


class PVTrain:
    '''
    Create class to extract passenger volume by train data from monthly zip files.
    '''

    def __init__(self, date):
        self.date = date
        self.conf = util.load_config('config.yaml')
        self.conf_pvt = util.load_config('lta_pv_train.yaml')

    def unzip_to_incoming(self,
                          zip_dir: str,
                          csv_dir: str):
        '''
        Function unzips the PV Train file from S3 and stores the csv into S3.

        Parameters
        ----------
            zip_dir: dir that stores the zip file
            csv_dir: dir that stores the csv file
        '''

        yyyymm = dt.strftime(self.date, '%Y%m')

        try:
            bucket = os.environ.get('BUCKET')
            s3_client = boto3.client('s3')

            # Resolve by month so files downloaded on different days still match.
            zip_prefix = f'{zip_dir}/pv_train_{yyyymm}'
            matches = []
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=zip_prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith('.zip'):
                        matches.append((obj['LastModified'], key))

            if not matches:
                err_msg = f'No zip files found for prefix s3://{bucket}/{zip_prefix}'
                logger.error(err_msg)
                raise FileNotFoundError(err_msg)

            zip_key = max(matches, key=lambda x: x[0])[1]
            obj = s3_client.get_object(Bucket=bucket, Key=zip_key)
            zip_bytes = obj['Body'].read()

            with zipfile.ZipFile(BytesIO(zip_bytes), 'r') as zip_obj:
                for fname in zip_obj.namelist():
                    path = PurePosixPath(fname)
                    if path.is_absolute() or '..' in path.parts:
                        err_msg = f'Unsafe file detected in {zip_key}: {fname}'
                        logger.error(err_msg)
                        raise Exception(err_msg)

                    # save data into s3
                    data = BytesIO(zip_obj.read(fname))
                    csv_key = f'{csv_dir}/{fname}'
                    util_s3.upload_fileobj(zip_resp=data,
                                           object_name=csv_key)
                    logger.info(f'{fname} extracted to s3://{bucket}/{csv_key}')

            logger.info(f'Successfully unzipped file s3://{bucket}/{zip_key}.')
        except Exception as e:
            logger.error(f'The error {e} occurred.')
            raise


if __name__ == "__main__":

    curr_date = dt.now().date()
    pv_train = PVTrain(curr_date)

    zip_prefix = 'incoming/pv_train/zip'
    csv_prefix = 'incoming/pv_train/csv'
    pv_train.unzip_to_incoming(zip_prefix, csv_prefix)
