#!/usr/bin/env python3

import requests
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
    Create class to perform API call and download passenger volume by train
    stations data from LTA DataMall
    '''

    def __init__(self, date):
        self.date = date
        self.conf = util.load_config('config.yaml')
        self.conf_pvt = util.load_config('lta_pv_train.yaml')

    def api_call(self):
        '''
        API call to LTA DataMall to get URL for the data.
        '''

        key = os.environ['LTA_KEY']

        url = self.conf['api']['lta_url']
        url_suffix = self.conf_pvt['config_pv_train']['url_suffix']

        headers = {
            'AccountKey': key,
            'accept': 'application/json'
        }

        resp = requests.get(url + url_suffix,
                            headers=headers,
                            stream=True)

        if resp.ok:
            data = resp.json()
            dl_link = data['value'][0]['Link']
            logger.info(f'api call: {url_suffix}: {resp.status_code}')
        else:
            err = f'Error: {resp.status_code}, {resp.text}'
            logger.error(f'{err}')
            raise Exception(f'{err}')

        return dl_link

    def download_zip(self,
                     dl_link: str,
                     zip_dir: str):
        '''
        Download zip file from URL and upload directly to S3.

        Parameters
        ----------
            dl_link: url link to the download
            zip_dir: dir that stores the zip file
        '''

        yyyymmdd = dt.strftime(self.date, '%Y%m%d')
        file_name = f'pv_train_{yyyymmdd}.zip'

        zip_resp = requests.get(dl_link, stream=True)

        if zip_resp.ok:
            util_s3.upload_fileobj(zip_resp.raw,
                                   f'{zip_dir}/{file_name}')
            logger.info(f'Successfully wrote {file_name} to s3.')
        else:
            err = f'Error: {zip_resp.status_code}, {zip_resp.text}'
            logger.error(f'{err}')
            raise Exception(f'{err}')

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

        yyyymmdd = dt.strftime(self.date, '%Y%m%d')

        try:
            bucket = os.environ.get('BUCKET')
            s3_client = boto3.client('s3')

            zip_key = f'{zip_dir}/pv_train_{yyyymmdd}.zip'
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
    # do api call to lta to get zip link
    pv_train = PVTrain(curr_date)

    # print("Testing unzip_to_incoming()...")
    yyyymmdd = dt.strftime(curr_date, '%Y%m%d')
    zip_prefix = 'incoming/pv_train/zip'
    csv_prefix = 'incoming/pv_train/csv'
    pv_train.unzip_to_incoming(zip_prefix, csv_prefix)
