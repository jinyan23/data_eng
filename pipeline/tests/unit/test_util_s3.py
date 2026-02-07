#!/usr/bin/env python3

from io import BytesIO
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

import util_s3


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv('BUCKET', 'test-bucket')


@patch('util_s3.boto3.client')
def test_upload_file_uses_basename_when_object_name_none(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3

    result = util_s3.upload_file('/tmp/test.csv')

    assert result is True
    mock_s3.upload_file.assert_called_once_with(
        '/tmp/test.csv',
        'test-bucket',
        'test.csv'
    )


@patch('util_s3.boto3.client')
def test_upload_file_returns_false_on_error(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.upload_file.side_effect = ClientError(
        {'Error': {'Code': '500', 'Message': 'fail'}},
        'upload_file',
    )

    result = util_s3.upload_file('/tmp/test.csv', 'key.csv')

    assert result is False


@patch('util_s3.boto3.client')
def test_upload_fileobj_success(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3

    data = BytesIO(b'hello')
    result = util_s3.upload_fileobj(data, 'key.csv')

    assert result is True
    mock_s3.upload_fileobj.assert_called_once_with(
        data,
        'test-bucket',
        'key.csv'
    )


@patch('util_s3.boto3.client')
def test_upload_fileobj_returns_false_on_error(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.upload_fileobj.side_effect = ClientError(
        {'Error': {'Code': '500', 
                   'Message': 'fail'}},
        'upload_fileobj',
    )

    data = BytesIO(b'hello')
    result = util_s3.upload_fileobj(data, 'key.csv')

    assert result is False


@patch('util_s3.boto3.client')
def test_read_csv_s3(mock_client):
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3
    mock_s3.get_object.return_value = {
        'Body': BytesIO(b'col1,col2\n1,2\n')
    }

    df = util_s3.read_csv_s3('path/to/file.csv')

    mock_s3.get_object.assert_called_once_with(
        Bucket='test-bucket',
        Key='path/to/file.csv'
    )
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['col1', 'col2']
    assert df.iloc[0].tolist() == [1, 2]
