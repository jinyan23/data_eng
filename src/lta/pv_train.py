#!/usr/bin/env python3

import requests
from datetime import datetime as dt
import shutil

import util
from util import UDLogger

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

        with util.safe_open(self.conf['api']['lta_key'], 'r') as k:
            key = k.readlines()[0]

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
        Download zip file from URL and deposit file into incoming dir.

        Parameters
        ----------
            dl_link: url link to the download
            zip_dir: dir that stores the zip file
        '''

        yyyymmdd = dt.strftime(self.date, '%Y%m%d')
        zip_path = f'{zip_dir}/pv_train_{yyyymmdd}.zip'

        zip_resp = requests.get(dl_link)

        if zip_resp.ok:
            with util.safe_open(zip_path, 'wb') as f:
                for chunk in zip_resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            logger.info(f'Successfully wrote zip file to {zip_path}')
        else:
            err = f'Error: {zip_resp.status_code}, {zip_resp.text}'
            logger.error(f'{err}')
            raise Exception(f'{err}')

    def unzip_to_incoming(self,
                          zip_dir: str,
                          csv_dir: str,
                          arv_dir: str | bool = False):
        '''
        Function unzips the PV Train file and store the csv into the csv file.
        It then moves the zip file into the archive folder.

        Parameters
        ----------
            zip_dir: dir that stores the zip file
            csv_dir: dir that stores the csv file
            arv_dir: archive dir or if not archiving, False
        '''

        yyyymmdd = dt.strftime(self.date, '%Y%m%d')
        zip_path = f'{zip_dir}/pv_train_{yyyymmdd}.zip'
        arv_path = f'{arv_dir}/pv_train_{yyyymmdd}.zip'

        try:
            util.unzip_file(zip_path, csv_dir, logger)
            logger.info(f'Successfully unzipped file {zip_path}.')
            if arv_dir is not False:
                shutil.move(zip_path, arv_path)
                logger.info(f'Successfully moved zipped file to {arv_path}.')
        except Exception as e:
            logger.error(f'The error {e} occurred.')
            raise
