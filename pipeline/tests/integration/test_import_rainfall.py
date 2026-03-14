#!/usr/bin/env python3

from datetime import datetime as dt
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from rainfall.import_rainfall import import_rainfall


@pytest.fixture
def mock_env(monkeypatch):
    env = {
        'DB_HOST': '127.0.0.1',
        'DB_PORT': '3307',
        'DB_USER': 'test_username',
        'DB_PASS': 'test_password',
        'DB_NAME': 'test_database',
        'BUCKET': 'test-bucket',
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return env


@patch('rainfall.import_rainfall.config_pv', {
    'csv_prefix': 'incoming/rainfall',
    'csv_name': 'rainfall',
    'delimiter': '|',
    'mode': 'append',
    'timekey': 'timestamp',
    'col_pd': {
        'timestamp': 'str',
        'stationId': 'str',
        'value': 'float64',
        'id': 'str',
        'deviceId': 'str',
        'name': 'str',
        'location.latitude': 'float64',
        'location.longitude': 'float64',
    }
})
@patch('rainfall.import_rainfall.config_db_tbl', {
    'schema': 'weather',
    'tbl': 'r_rainfall',
    'tbl_col': {
        'timestamp': 'timestamp',
        'stationId': 'station_id',
        'value': 'value',
        'id': 'nea_id',
        'deviceId': 'device_id',
        'name': 'name',
        'location.latitude': 'loc_lat',
        'location.longitude': 'loc_lon',
    }
})
@patch('rainfall.import_rainfall.DataPipe')
@patch('rainfall.import_rainfall.util_s3.write_csv_s3')
@patch('rainfall.import_rainfall.boto3.client')
def test_import_rainfall_loads_new_file(
    mock_boto_client,
    mock_write_csv_s3,
    mock_datapipe,
    mock_env,
):
    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client

    registry_key = 'incoming/rainfall/registry.csv'
    data_key = 'incoming/rainfall/rainfall_20260314_101500.csv'
    csv_bytes = (
        b'timestamp|stationId|value|id|deviceId|name|location.latitude|location.longitude\n'
        b'2026-03-14 10:15:00|S1|0|S1|S1|Station 1|1.3|103.8\n'
    )

    def get_object(Bucket, Key):
        if Key == registry_key:
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        return {'Body': BytesIO(csv_bytes)}

    mock_client.get_object.side_effect = get_object
    mock_client.list_objects_v2.return_value = {
        'Contents': [{'Key': data_key, 'LastModified': dt(2026, 3, 14)}],
        'IsTruncated': False,
    }

    mock_instance = MagicMock()
    mock_datapipe.return_value = mock_instance

    import_rainfall()

    mock_datapipe.assert_called_once_with(
        hostname=mock_env['DB_HOST'],
        username=mock_env['DB_USER'],
        password=mock_env['DB_PASS'],
        database=mock_env['DB_NAME'],
        port=mock_env['DB_PORT'],
    )
    mock_instance.load_db.assert_called_once_with(
        config_db={
            'schema': 'weather',
            'tbl': 'r_rainfall',
            'tbl_col': {
                'timestamp': 'timestamp',
                'stationId': 'station_id',
                'value': 'value',
                'id': 'nea_id',
                'deviceId': 'device_id',
                'name': 'name',
                'location.latitude': 'loc_lat',
                'location.longitude': 'loc_lon',
            }
        },
        mode='append',
        timekey='timestamp',
        yyyymm='202603',
        data=[
            ('2026-03-14 10:15:00', 'S1', 0.0, 'S1', 'S1', 'Station 1', 1.3, 103.8),
        ]
    )
    assert mock_write_csv_s3.called
    args, kwargs = mock_write_csv_s3.call_args
    assert args[1] == registry_key
    assert kwargs['index'] is False
