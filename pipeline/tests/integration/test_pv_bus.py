#!/usr/bin/env python3

import io
from io import BytesIO
from datetime import datetime as dt
import pytest
import responses
import zipfile
from unittest.mock import patch, MagicMock

from lta.pv_bus import PVBus


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    env = {
        'LTA_KEY': 'api_token',
        'BUCKET': 'test-bucket',
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return env


@responses.activate
def test_pv_bus_api_call_pass():

    url = 'https://datamall2.mytransport.sg/ltaodataservice/PV/Bus'
    pvb = PVBus(dt.now().date())

    resp_pass = responses.Response(
        method='GET',
        url=url,
        json={'value': [{'Link': 'download_link'}, {'Link2': 'url2'}],
              'next_value': 'test_line'},
        status=200
    )

    responses.add(resp_pass)
    dl_link = pvb.api_call()

    # assert json object is parsed correctly
    assert dl_link == 'download_link'


@responses.activate
def test_pv_bus_api_call_fail():

    url = 'https://datamall2.mytransport.sg/ltaodataservice/PV/Bus'
    pvb = PVBus(dt.now().date())

    resp_fail = responses.Response(
        method='GET',
        url=url,
        status=403
    )

    responses.add(resp_fail)

    # assert error status code can be obtained correctly
    with pytest.raises(Exception) as excinfo:
        pvb.api_call()
    assert "Error: 403" in str(excinfo.value)


@responses.activate
@patch('lta.pv_bus.util_s3.upload_fileobj')
def test_pv_bus_download_zip_pass(mock_upload):

    url = 'https://datamall2.mytransport.sg/ltaodataservice/PV/Bus'
    pvb = PVBus(dt.now().date())
    yyyymmdd = dt.strftime(pvb.date, '%Y%m%d')
    zip_content = b'zipcontent_byte'

    resp_pass = responses.Response(
        method='GET',
        url=url,
        body=zip_content,
        status=200,
        content_type='application/octet-stream'
    )

    responses.add(resp_pass)

    zip_dir = 'incoming/pv_bus/zip'
    pvb.download_zip(url, zip_dir)
    test_zip = f'pv_bus_{yyyymmdd}.zip'

    mock_upload.assert_called_once()
    args, kwargs = mock_upload.call_args
    object_name = kwargs.get('object_name', args[1] if len(args) > 1 else None)
    assert object_name == f'{zip_dir}/{test_zip}'


@responses.activate
@patch('lta.pv_bus.util_s3.upload_fileobj')
def test_pv_bus_download_zip_fail(mock_upload):

    url = 'https://datamall2.mytransport.sg/ltaodataservice/PV/Bus'
    pvb = PVBus(dt.now().date())
    zip_content = b'zipcontent_byte'

    resp_fail = responses.Response(
        method='GET',
        url=url,
        body=zip_content,
        status=404,
        content_type='application/octet-stream'
    )

    responses.add(resp_fail)

    # assert error status code can be obtained correctly
    with pytest.raises(Exception) as excinfo:
        pvb.download_zip(url, 'incoming/pv_bus/zip')
    assert "Error: 404" in str(excinfo.value)
    mock_upload.assert_not_called()


@patch('lta.pv_bus.util_s3.upload_fileobj')
@patch('lta.pv_bus.boto3.client')
def test_pv_bus_unzip_to_incoming_pass(mock_boto_client, mock_upload):

    pvb = PVBus(dt.now().date())
    yyyymmdd = dt.strftime(pvb.date, '%Y%m%d')

    test_name = 'pv_bus'
    csv_filename = f'{test_name}_{yyyymmdd}.csv'
    zip_filename = f'{test_name}_{yyyymmdd}.zip'

    # create a temp csv and zip file
    mock_data = 'col1,col2,col3\n1,2,3\n4,5,6'
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr(csv_filename, mock_data)

    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.get_object.return_value = {
        'Body': BytesIO(zip_buffer.getvalue())
    }

    pvb.unzip_to_incoming(
        csv_dir='incoming/pv_bus/csv',
        zip_dir='incoming/pv_bus/zip'
    )

    mock_s3.get_object.assert_called_once_with(
        Bucket='test-bucket',
        Key=f'incoming/pv_bus/zip/{zip_filename}'
    )
    mock_upload.assert_called_once()
    _, kwargs = mock_upload.call_args
    assert kwargs['object_name'] == f'incoming/pv_bus/csv/{csv_filename}'


@patch('lta.pv_bus.util_s3.upload_fileobj')
@patch('lta.pv_bus.boto3.client')
def test_pv_bus_unzip_to_incoming_path_traversal(
    mock_boto_client,
    mock_upload
):
    pvb = PVBus(dt.now().date())
    yyyymmdd = dt.strftime(pvb.date, '%Y%m%d')
    zip_filename = f'pv_bus_{yyyymmdd}.zip'

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr('../evil.csv', 'malicious')

    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.get_object.return_value = {
        'Body': BytesIO(zip_buffer.getvalue())
    }

    with pytest.raises(Exception) as excinfo:
        pvb.unzip_to_incoming(
            csv_dir='incoming/pv_bus/csv',
            zip_dir='incoming/pv_bus/zip'
        )

    assert f'Unsafe file detected in incoming/pv_bus/zip/{zip_filename}' in str(excinfo.value)
    mock_upload.assert_not_called()


@patch('lta.pv_bus.boto3.client')
def test_pv_bus_unzip_to_incoming_fail(mock_boto_client):

    pvb = PVBus(dt.now().date())
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    mock_s3.get_object.side_effect = Exception('missing')

    # assert error status code can be obtained correctly
    with pytest.raises(Exception):
        pvb.unzip_to_incoming(
            csv_dir='incoming/pv_bus/csv',
            zip_dir='incoming/pv_bus/zip'
        )
