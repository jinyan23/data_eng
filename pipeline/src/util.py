#!/usr/bin/env python3

'''Utility Functions'''

import os

import zipfile
import logging
import yaml


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

    # look for config directory 
    config_dir = os.getenv('CONFIG_DIR')

    if not config_dir:
        config_dir = os.path.expanduser('~/pipeline/config')

    config_path = os.path.join(config_dir, config_file)

    with open(f'{config_path}', 'r', encoding='utf-8') as f:
        conf = yaml.safe_load(f)

    return conf


def unzip_file(zip_path, out_dir, logger):
    '''
    Utility function to unzip and save a file in a specified dir.

    Parameters
    ----------
        zip_path (str): path to the zip file
        out_dir (str): path to the destination directory
    '''
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_obj:
            for fname in zip_obj.namelist():

                # check for path traversal
                abs_path = os.path.abspath(os.path.join(out_dir, fname))
                if not abs_path.startswith(os.path.abspath(out_dir)):
                    err_msg = f'Unsafe file detected in {zip_path}: {fname}'
                    logger.error(err_msg)
                    raise Exception(err_msg)

                # extract files to destination directory
                zip_obj.extract(fname, out_dir)
                logger.info(f'{fname} extracted to: {out_dir}')

    except zipfile.BadZipFile:
        raise Exception(f'Error: {zip_path} is not a valid zip file.')


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
        # create directory to hold .log files
        log_dir = os.path.expanduser('~/logs')
        os.makedirs(log_dir, exist_ok=True)

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
