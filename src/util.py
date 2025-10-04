#!/usr/bin/env python3

'''Utility Functions'''

import zipfile
import logging
import os
import yaml
from datetime import datetime as dt


def safe_open(path: str, mode: str):
    '''
    Utility function to expand ~/ home directory safely

    Parameters
    ----------
        path (str): path to the file
        mode (str): 'wb' or 'r' mode used to open the file
    '''

    full_path = os.path.expanduser(path)

    return open(full_path, mode)


def load_config(config_file: str):
    '''
    Utility function to load the requested config files.

    Parameters
    ----------
        config_file (str): config file name
    '''

    config_path = os.path.expanduser(f'~/data_eng/config/{config_file}')

    with open(f'{config_path}', 'r', encoding='utf-8') as f:
        conf = yaml.safe_load(f)

    return conf


def unzip_file(file_dir, dir_out):
    '''
    Utility function to unzip and save a file in a specified dir.

    Parameters
    ----------
        file_dir (str): path to the zip file
        dir_out (str): path to the destination directory
    '''
    try:
        with zipfile.ZipFile(file_dir, 'r') as zip_obj:
            for fname in zip_obj.namelist():

                # Check for path traversal
                abs_path = os.path.abspath(os.path.join(dir_out, fname))
                if not abs_path.startswith(os.path.abspath(dir_out)):
                    raise ValueError(f'Unsafe file detected in zip: {fname}')

                # Extract all files to destination directory
                zip_obj.extractall(dir_out)
                print(f'All files saved in: {dir_out}')

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
