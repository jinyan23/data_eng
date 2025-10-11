#!/usr/bin/env python3

import zipfile
import util
from util import UDLogger

ud_logger = UDLogger(filename='test.log', name=__name__)
logger = ud_logger.create_logger()


def test_unzip_file(tmp_path):
    '''
    Perform unit test for util.unzip_file() function.
    1) File to be successfully written into outgoing dir
    2) Data present in the file can be read successfully
    '''

    # create a temp csv and zip file
    test_name = 'test_unzip_file'
    mock_data = '''col1, col2, col3'''

    csv_path = tmp_path / f'{test_name}.csv'
    zip_path = tmp_path / f'{test_name}.zip'
    out_dir = tmp_path / 'outgoing'

    with open(csv_path, 'w') as f:
        f.write(mock_data)

    with zipfile.ZipFile(zip_path, 'w') as f:
        f.write(csv_path, arcname=f'{test_name}.csv')

    # function to test
    util.unzip_file(zip_path, out_dir, logger)

    # check if file got successfully unzipped
    extracted_path = out_dir / f'{test_name}.csv'
    assert extracted_path.exists()
    assert extracted_path.read_text() == mock_data


def test_unzip_path_traversal(tmp_path):
    '''
    Perform unit test for path traversal malicious file catch.
    1) Malicious file path e.g. '../malicious.txt' will return an exception
    2) No file will be written out
    '''
    # create a temp malicious zip file
    malicious_zip_path = tmp_path / 'malicious.zip'
    out_dir = tmp_path / 'outgoing'

    with zipfile.ZipFile(malicious_zip_path, 'w') as zfile:
        zfile.writestr('../malicious.txt', "malicious content")

    # run and expect an Exception
    try:
        util.unzip_file(malicious_zip_path, out_dir, logger)
    except Exception as e:
        assert "Unsafe file detected" in str(e)

    # ensure no files were extracted
    extracted_files = list(out_dir.glob("**/*"))
    assert len(extracted_files) == 0


def test_unzip_file_bad_zip(tmp_path):
    '''
    Perform unit test for invalid zip path.
    1) Invalid zip file will return an exception
    '''
    bad_zip_path = tmp_path / "bad.zip"
    bad_zip_path.write_text("invalid zip file")
    try:
        util.unzip_file(str(bad_zip_path), str(tmp_path), logger)
    except Exception as e:
        assert "not a valid zip file" in str(e)


def test_udlogger_init():
    '''
    Perform unit test for user defined logger.
    1) Able to instantiate UDLogger class object
    '''
    logger = UDLogger(filename='test.log',
                      name='test_logger')
    assert logger.filename.endswith('test.log')
