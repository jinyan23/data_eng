#!/usr/bin/env python3

import pandas as pd
import yaml
import json
import requests
from urllib.parse import urlparse

with open('config/config.yaml', 'r') as f:
    conf = yaml.safe_load(f)

with open(conf['api']['lta_key']) as k:
    key = k.readlines()[0]

def main():
    
    url = conf['api']['lta_url']
    url_suffix = 'ltaodataservice/PV/Train'
    
    headers = {
        'AccountKey': key,
        'accept': 'application/json'
    }

    response = requests.get(url + url_suffix, headers=headers)
    
    if response.ok:
        data = response.json()
        print(data)
    else:
        print('Error:', response.status_code, response.text)


    return data

if __name__=='__main__':
    
    obj = main()
