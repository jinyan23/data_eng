#!/usr/bin/env python3

'''Utility Functions'''

import zipfile
import logging
import os
import yaml
from datetime import datetime as dt

def load_config(config_file: str):
    '''
    Utiliy function to load the requested config files.

    Parameters
    ----------
        config_fig (str): config file name 
    '''

    config_path = os.path.expanduser(f'~/data_eng/config/{config_file}')

    with open(f'{config_path}', 'r', encoding='utf-8') as f:
        conf = yaml.safe_load(f)

    return conf

def unzip_file(file_dir, destination_dir):
    '''
    Utility function to unzip and save a file in a specified dir.

    Parameters
    ----------
        file_dir (str): path to the zip file
        destination_dir (str): path to the destination directory
    '''
    try:
        with zipfile.ZipFile(file_dir, 'r') as zip_obj:
            for file_name in zip_obj.namelist():

                # Check for path traversal
                abs_path = os.path.abspath(os.path.join(destination_dir, file_name))
                if not abs_path.startswith(os.path.abspath(destination_dir)):
                    raise ValueError(f'Unsafe file detected in zip: {file_name}')

                # Extract all files to destination directory
                zip_obj.extractall(destination_dir)
                print(f'All files saved in: {destination_dir}')

    except zipfile.BadZipFile:
        print(f'Error: {file_dir} is not a valid zip file.')

def time_now():
    '''
    Returns time now for logging of script execution.
    '''
    time_now_dt = dt.strftime(dt.now(), '%Y-%m-%d %H:%M:%S')
    time_now_str = time_now_dt + '\t'

    return time_now_str

class UDLogger:
    '''
    User defined logger class.
    Parameters
    ----------
        filename (str): .log file the log messages to be written to
        name (str): name of the logger (typically the module's name)
    '''
    def __init__(self, filename: str, name: str):
        self.filename = os.path.expanduser(f'~/logs/{filename}')
        self.mode = 'a'
        self.encoding = 'utf-8'
        self.name = name

    def create_logger(self):
        '''
        Create new logger. 

        Parameters
        ----------
            name (str): logger name
        '''
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)

        # create handlers
        console_handler = logging.StreamHandler()
        info_handler = logging.FileHandler(
            filename=self.filename,
            mode=self.mode,
            encoding=self.encoding
        )

        # logging format
        formatter = logging.Formatter(
            fmt='%(asctime)s: %(levelname)s:%(name)s:%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ) 

        # assignment of formatter to handler and handler to logger
        console_handler.setFormatter(formatter)
        info_handler.setFormatter(formatter)

        logger.addHandler(console_handler)
        logger.addHandler(info_handler)

        return logger
