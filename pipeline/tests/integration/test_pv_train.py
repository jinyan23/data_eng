#!/usr/bin/env python3

import io
from io import BytesIO
from datetime import datetime as dt
from unittest.mock import patch, MagicMock

import pytest
import zipfile

from pv_train.extract_pv_train import PVTrain


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    env = {
        'LTA_KEY': 'api_token',
        'BUCKET': 'test-bucket',
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return env


@patch('pv_train.extract_pv_train.util_s3.upload_fileobj')
@patch('pv_train.extract_pv_train.boto3.client')
def test_pv_train_unzip_to_incoming_pass(mock_boto_client, mock_upload):
    pvt = PVTrain(dt(2026, 3, 7).date())
    yyyymm = dt.strftime(pvt.date, '%Y%m')

    csv_filename = 'pv_train_20260305.csv'
    latest_zip = f'incoming/pv_train/zip/pv_train_{yyyymm}05.zip'

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr(csv_filename, 'col1,col2,col3\n1,2,3\n4,5,6')

    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{
        'Contents': [
            {'Key': f'incoming/pv_train/zip/pv_train_{yyyymm}01.zip', 'LastModified': dt(2026, 3, 1)},
            {'Key': latest_zip, 'LastModified': dt(2026, 3, 5)},
        ]
    }]
    mock_s3.get_object.return_value = {'Body': BytesIO(zip_buffer.getvalue())}

    pvt.unzip_to_incoming(zip_dir='incoming/pv_train/zip', csv_dir='incoming/pv_train/csv')

    mock_s3.get_object.assert_called_once_with(Bucket='test-bucket', Key=latest_zip)
    mock_upload.assert_called_once()
    _, kwargs = mock_upload.call_args
    assert kwargs['object_name'] == f'incoming/pv_train/csv/{csv_filename}'


@patch('pv_train.extract_pv_train.util_s3.upload_fileobj')
@patch('pv_train.extract_pv_train.boto3.client')
def test_pv_train_unzip_to_incoming_path_traversal(mock_boto_client, mock_upload):
    pvt = PVTrain(dt(2026, 3, 7).date())
    yyyymm = dt.strftime(pvt.date, '%Y%m')
    zip_key = f'incoming/pv_train/zip/pv_train_{yyyymm}05.zip'

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr('../evil.csv', 'malicious')

    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{
        'Contents': [{'Key': zip_key, 'LastModified': dt(2026, 3, 5)}]
    }]
    mock_s3.get_object.return_value = {'Body': BytesIO(zip_buffer.getvalue())}

    with pytest.raises(Exception, match=f'Unsafe file detected in {zip_key}'):
        pvt.unzip_to_incoming(zip_dir='incoming/pv_train/zip', csv_dir='incoming/pv_train/csv')
    mock_upload.assert_not_called()


@patch('pv_train.extract_pv_train.boto3.client')
def test_pv_train_unzip_to_incoming_fail_when_no_monthly_zip(mock_boto_client):
    pvt = PVTrain(dt(2026, 3, 7).date())
    mock_s3 = MagicMock()
    mock_paginator = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [{'Contents': []}]

    with pytest.raises(FileNotFoundError, match='No zip files found for prefix'):
        pvt.unzip_to_incoming(zip_dir='incoming/pv_train/zip', csv_dir='incoming/pv_train/csv')
