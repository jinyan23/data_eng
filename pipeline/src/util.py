#!/usr/bin/env python3

'''Utility Functions'''

import os

import requests
from datetime import datetime as dt
from pathlib import Path
import yaml

import util_s3
from util_logger import UDLogger

ud_logger = UDLogger(filename='api.log', name=__name__)
logger = ud_logger.create_logger()

# define root dir
root = Path(__file__).parent.parent


def safe_open(path: str, mode: str):
    '''
    Utility function to expand from root directory safely

    Parameters
    ----------
        path (str): path to the file
        mode (str): 'wb' or 'r' mode used to open the file
    '''

    full_path = root / path

    return open(full_path, mode)


def load_config(config_file: str):
    '''
    Utility function to load the requested config files.

    Parameters
    ----------
        config_file (str): config file name
    '''

    config_path = root / 'config' / config_file

    with open(config_path, 'r', encoding='utf-8') as f:
        conf = yaml.safe_load(f)

    return conf


def api_call(data_config):
    '''
    API call to LTA DataMall to get URL for the data.
    '''

    key = os.environ['LTA_KEY']

    url = load_config('config.yaml')['api']['lta_url']
    url_suffix = data_config['config_proc']['url_suffix']

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


def download_zip(curr_date,
                 file_name: str,
                 dl_link: str,
                 zip_dir: str):
    '''
    Download zip file from URL and upload directly to S3.

    Parameters
    ----------
        curr_date: current date
        file_name: prefix of the file name
        dl_link: url link to the download
        zip_dir: dir that stores the zip file
    '''

    yyyymmdd = dt.strftime(curr_date, '%Y%m%d')
    file_name = f'{file_name}_{yyyymmdd}.zip'

    zip_resp = requests.get(dl_link, stream=True)

    if zip_resp.ok:
        util_s3.upload_fileobj(zip_resp.raw,
                               f'{zip_dir}/{file_name}')
        logger.info(f'Successfully wrote {file_name} to s3.')
    else:
        err = f'Error: {zip_resp.status_code}, {zip_resp.text}'
        logger.error(f'{err}')
        raise Exception(f'{err}')
