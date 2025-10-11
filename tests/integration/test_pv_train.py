#!/usr/bin/env python3

import io
from datetime import datetime as dt
import pytest
import responses
import zipfile

from lta.pv_train import PVTrain


# api_call() test
@responses.activate
def test_pv_train_api_call_pass():

    url = 'https://datamall2.mytransport.sg/ltaodataservice/PV/Train'
    pvt = PVTrain(dt.now().date())

    resp_pass = responses.Response(
        method='GET',
        url=url,
        json={'value': [{'Link': 'download_link'}, {'Link2': 'url2'}],
              'next_value': 'test_line'},
        status=200
    )

    responses.add(resp_pass)
    dl_link = pvt.api_call()

    # assert json object is parsed correctly
    assert dl_link == 'download_link'


@responses.activate
def test_pv_train_api_call_fail():

    url = 'https://datamall2.mytransport.sg/ltaodataservice/PV/Train'
    pvt = PVTrain(dt.now().date())

    resp_fail = responses.Response(
        method='GET',
        url=url,
        status=403
    )

    responses.add(resp_fail)

    # assert error status code can be obtained correctly
    with pytest.raises(Exception) as excinfo:
        pvt.api_call()
    assert "Error: 403" in str(excinfo.value)


@responses.activate
def test_pv_train_download_zip_pass(tmp_path):

    url = 'https://datamall2.mytransport.sg/ltaodataservice/PV/Train'
    pvt = PVTrain(dt.now().date())
    yyyymmdd = dt.strftime(pvt.date, '%Y%m%d')
    zip_content = b'zipcontent_byte'

    resp_pass = responses.Response(
        method='GET',
        url=url,
        body=zip_content,
        status=200,
        content_type='application/octet-stream'
    )

    responses.add(resp_pass)

    pvt.download_zip(url, tmp_path)
    test_zip = f'pv_train_{yyyymmdd}.zip'
    zip_path = tmp_path / f'{test_zip}'

    # assert that zip file is downloaded and stored
    assert zip_path.exists()

    # assert that the content of the zip file is uncorrupted
    with open(zip_path, "rb") as f:
        assert f.read() == zip_content


@responses.activate
def test_pv_train_download_zip_fail(tmp_path):

    url = 'https://datamall2.mytransport.sg/ltaodataservice/PV/Train'
    pvt = PVTrain(dt.now().date())
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
        pvt.download_zip(url, tmp_path)
    assert "Error: 404" in str(excinfo.value)


@responses.activate
def test_pv_train_unzip_to_incoming_pass(tmp_path):

    pvt = PVTrain(dt.now().date())
    yyyymmdd = dt.strftime(pvt.date, '%Y%m%d')

    test_name = 'pv_train'
    csv_filename = f'{test_name}_{yyyymmdd}.csv'
    zip_filename = f'{test_name}_{yyyymmdd}.zip'
    csv_path = tmp_path / csv_filename
    zip_path = tmp_path / zip_filename
    arv_path = tmp_path / zip_filename

    # create a temp csv and zip file
    mock_data = 'col1,col2,col3\n1,2,3\n4,5,6'
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr(csv_filename, mock_data)

    with open(zip_path, 'wb') as f:
        f.write(zip_buffer.getvalue())

    pvt.unzip_to_incoming(
        csv_dir=tmp_path,
        zip_dir=tmp_path,
        arv_dir=tmp_path
    )

    # assert that csv file is written out correctly
    assert csv_path.exists()

    # assert that the csv data is uncorrupted after unzipping
    with open(csv_path) as f:
        assert f.read() == mock_data

    # assert that zip file is moved to archive dir correctly
    assert arv_path.exists()


@responses.activate
def test_pv_train_unzip_to_incoming_fail(tmp_path):

    pvt = PVTrain(dt.now().date())
    # yyyymmdd = dt.strftime(pvt.date, '%Y%m%d')
    # test_name = 'pv_train'
    # zip_filename = f'{test_name}_{yyyymmdd}.zip'
    # zip_path = tmp_path / zip_filename  # Do NOT create this file

    # assert error status code can be obtained correctly
    with pytest.raises(FileNotFoundError):
        pvt.unzip_to_incoming(
            csv_dir=tmp_path,
            zip_dir=tmp_path,
            arv_dir=tmp_path
        )
