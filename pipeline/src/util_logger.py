#!/usr/bin/env python3

import os
import logging


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
