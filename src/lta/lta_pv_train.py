#!/usr/bin/env python3

import yaml
import requests
from datetime import datetime as dt
import util


with open('config/config.yaml', 'r') as f:
    conf = yaml.safe_load(f)

with open(conf['api']['lta_key']) as k:
    key = k.readlines()[0]


class PVTrain:
    '''
    Create class to perform API call and download passenger volume by train 
    stations data from LTA DataMall 
    '''

    def __init__(self, date):
        self.date = date


    def api_call(self):
        '''
        API call to LTA DataMall to get URL for the data.
        '''

        url = conf['api']['lta_url']
        url_suffix = 'ltaodataservice/PV/Train'
        
        headers = {
            'AccountKey': key,
            'accept': 'application/json'
        }

        response = requests.get(url + url_suffix, headers=headers, stream=True)
        
        if response.ok:
            data = response.json()
            dl_link = data['value'][0]['Link']
            print(dl_link)
        else:
            print('Error:', response.status_code, response.text)
        
        return dl_link

    def download_zip(self, dl_link):
        '''
        Download zip file from URL and deposit file into incoming dir.
        '''

        yyyymmdd = dt.strftime(self.date, '%Y%m%d')
        zip_path = f'{conf['incoming']['pv_train']}zip/pv_train_{yyyymmdd}.zip'

        zip_response = requests.get(dl_link)

        with open(zip_path, "wb") as f:
            for chunk in zip_response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        return zip_response

    def unzip_to_incoming(file_dir: str, destination_dir: str):
        '''
        
        '''
        file_dir = file_dir
        destination_dir = destination_dir

        try:
            util.unzip_file(file_dir, destination_dir)
            print('Successfully unzip file.')
        except:
            print('Error in unzipping file.')

        


if __name__=='__main__':
    
    instance = PVTrain(dt.today().date())

    # dl_link = instance.api_call()
    # zip_response = instance.download_zip(dl_link = dl_link)
    
    file_dir = '../incoming/pv_train/zip/pv_train_20250910.zip'
    destination_dir = '../incoming/pv_train/csv/'
    PVTrain.unzip_to_incoming(file_dir=file_dir,
                              destination_dir=destination_dir)