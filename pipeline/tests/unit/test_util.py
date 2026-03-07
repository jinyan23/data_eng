#!/usr/bin/env python3

from datetime import date
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

import util
from util_logger import UDLogger


def test_safe_open_uses_root(monkeypatch, tmp_path):
    monkeypatch.setattr(util, 'root', tmp_path)

    with util.safe_open('sample.txt', 'w') as handle:
        handle.write('hello')

    assert (tmp_path / 'sample.txt').read_text() == 'hello'


def test_load_config_reads_from_config_folder(monkeypatch, tmp_path):
    config_dir = tmp_path / 'config'
    config_dir.mkdir()
    (config_dir / 'sample.yaml').write_text('api:\n  key: value\n', encoding='utf-8')
    monkeypatch.setattr(util, 'root', tmp_path)

    config = util.load_config('sample.yaml')

    assert config == {'api': {'key': 'value'}}


@patch('util.requests.get')
@patch('util.load_config')
def test_api_call_success(mock_load_config, mock_get, monkeypatch):
    monkeypatch.setenv('LTA_KEY', 'api_token')
    mock_load_config.return_value = {'api': {'lta_url': 'https://datamall2.mytransport.sg/'}}

    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.status_code = 200
    mock_resp.json.return_value = {'value': [{'Link': 'https://download-link'}]}
    mock_get.return_value = mock_resp

    conf = {'config_proc': {'url_suffix': 'ltaodataservice/PV/Bus'}}
    dl_link = util.api_call(conf)

    assert dl_link == 'https://download-link'
    mock_get.assert_called_once_with(
        'https://datamall2.mytransport.sg/ltaodataservice/PV/Bus',
        headers={'AccountKey': 'api_token', 'accept': 'application/json'},
        stream=True,
    )


@patch('util.requests.get')
@patch('util.load_config')
def test_api_call_failure(mock_load_config, mock_get, monkeypatch):
    monkeypatch.setenv('LTA_KEY', 'api_token')
    mock_load_config.return_value = {'api': {'lta_url': 'https://datamall2.mytransport.sg/'}}

    mock_resp = MagicMock()
    mock_resp.ok = False
    mock_resp.status_code = 403
    mock_resp.text = 'forbidden'
    mock_get.return_value = mock_resp

    conf = {'config_proc': {'url_suffix': 'ltaodataservice/PV/Train'}}

    with pytest.raises(Exception, match='Error: 403'):
        util.api_call(conf)


@patch('util.util_s3.upload_fileobj')
@patch('util.requests.get')
def test_download_zip_success(mock_get, mock_upload):
    mock_resp = MagicMock()
    mock_resp.ok = True
    mock_resp.raw = BytesIO(b'zip-bytes')
    mock_get.return_value = mock_resp

    util.download_zip(
        curr_date=date(2026, 3, 7),
        file_name='pv_bus',
        dl_link='https://download-link',
        zip_dir='incoming/pv_bus/zip',
    )

    mock_upload.assert_called_once_with(
        mock_resp.raw,
        'incoming/pv_bus/zip/pv_bus_20260307.zip',
    )


@patch('util.util_s3.upload_fileobj')
@patch('util.requests.get')
def test_download_zip_failure(mock_get, mock_upload):
    mock_resp = MagicMock()
    mock_resp.ok = False
    mock_resp.status_code = 404
    mock_resp.text = 'missing'
    mock_get.return_value = mock_resp

    with pytest.raises(Exception, match='Error: 404'):
        util.download_zip(
            curr_date=date(2026, 3, 7),
            file_name='pv_train',
            dl_link='https://download-link',
            zip_dir='incoming/pv_train/zip',
        )
    mock_upload.assert_not_called()


def test_udlogger_init():
    logger = UDLogger(filename='test.log', name='test_logger')
    assert logger.filename.endswith('test.log')
