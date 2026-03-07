#!/usr/bin/env python3

from datetime import datetime as dt
import util


def run_api_download(curr_date=None):
    '''
    Performs API call to download passenger volume by bus stations data from
    LTA DataMall.
    '''

    curr_date = curr_date or dt.now().date()

    yaml_file = 'lta_pv_bus.yaml'
    conf_pv = util.load_config(yaml_file)
    zip_dir = conf_pv['config_proc']['zip_prefix']
    file_name = zip_dir.split('/')[1]

    dl_link = util.api_call(conf_pv)
    util.download_zip(curr_date=curr_date,
                      file_name=file_name,
                      dl_link=dl_link,
                      zip_dir=zip_dir)


if __name__ == "__main__":
    run_api_download()
