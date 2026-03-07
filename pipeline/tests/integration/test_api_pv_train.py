#!/usr/bin/env python3

from datetime import date
from unittest.mock import patch

from pv_train.api_pv_train import run_api_download


@patch('pv_train.api_pv_train.util.download_zip')
@patch('pv_train.api_pv_train.util.api_call')
@patch('pv_train.api_pv_train.util.load_config')
def test_run_api_download_calls_util_functions_in_order(
    mock_load_config,
    mock_api_call,
    mock_download_zip,
):
    mock_conf = {'config_proc': {'zip_prefix': 'incoming/pv_train/zip'}}
    mock_load_config.return_value = mock_conf
    mock_api_call.return_value = 'https://example.com/train.zip'

    run_api_download(curr_date=date(2026, 3, 7))

    mock_load_config.assert_called_once_with('lta_pv_train.yaml')
    mock_api_call.assert_called_once_with(mock_conf)
    mock_download_zip.assert_called_once_with(
        curr_date=date(2026, 3, 7),
        file_name='pv_train',
        dl_link='https://example.com/train.zip',
        zip_dir='incoming/pv_train/zip',
    )
