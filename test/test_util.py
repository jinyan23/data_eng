#!/usr/bin/env python3

from src import util
import zipfile


def test_unzip_file(tmp_path):
    '''
    Perform unit test for util.unzip_file() function.
    '''

    # Create a temp csv and zip file
    test_name = 'test_unzip_file'
    mock_data = '''col1, col2, col3'''

    csv_path = tmp_path / f'{test_name}.csv'
    zip_path = tmp_path / f'{test_name}.zip'
    des_path = tmp_path / 'destination'

    with open(csv_path, 'w') as f:
        f.write(mock_data)

    with zipfile.ZipFile(zip_path, 'w') as f:
        f.write(csv_path, arcname=f'{test_name}.csv')

    # Function to test
    util.unzip_file(zip_path, des_path)

    # Check if file got successfully unzipped
    extracted_path = des_path / f'{test_name}.csv'
    assert extracted_path.exists()
    assert extracted_path.read_text() == mock_data
