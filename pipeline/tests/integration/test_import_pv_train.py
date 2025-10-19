#!/usr/bin/env python3

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from loaders.import_pv_train import import_pv_train


@pytest.fixture
def mock_env(monkeypatch):
    env = {
        'DB_HOST': '127.0.0.1',
        'DB_USER': 'test_username',
        'DB_PASS': 'test_password',
        'DB_NAME': 'test_database'
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    return env


@patch('loaders.import_pv_train.config_pv_train', {
    'backfill': False,
    'csv_prefix': '/tmp/incoming/pv_train/csv',
    'csv_name': 'csv_name',
    'yyyymm': '202501',
    'delimiter': ',',
    'col_pd': {
        'YEAR_MONTH': 'str',
        'DAY_TYPE': 'str',
        'TIME_PER_HOUR': 'int64',
        'PT_TYPE': 'str',
        'PT_CODE': 'str',
        'TOTAL_TAP_IN_VOLUME': 'int64',
        'TOTAL_TAP_OUT_VOLUME': 'int64'
    }
})
@patch('loaders.import_pv_train.config_db_tbl', {
    'tbl': 'r_pv_train',
    'tbl_col': {
        'YEAR_MONTH': 'year_month',
        'DAY_TYPE': 'day_type',
        'TIME_PER_HOUR': 'time_per_hour',
        'PT_TYPE': 'pt_type',
        'PT_CODE': 'pt_code',
        'TOTAL_TAP_IN_VOLUME': 'total_tap_in_volume',
        'TOTAL_TAP_OUT_VOLUME': 'total_tap_out_volume'
    }
})
@patch('loaders.import_pv_train.DataPipe')
@patch('loaders.import_pv_train.pd.read_csv')
def test_import_pv_train_backfill_F(mock_read_csv, mock_datapipe, mock_env):

    # mock datapipe
    mock_instance = MagicMock()
    mock_datapipe.return_value = mock_instance

    # mock csv data 
    mock_df = pd.DataFrame({
        'YEAR_MONTH': ['2025-01', '2024-12'],
        'DAY_TYPE': ['WEEKDAY', 'WEEKENDS/HOLIDAY'],
        'TIME_PER_HOUR': [20, 13],
        'PT_TYPE': ['TRAIN_A', 'TRAIN_B'],
        'PT_CODE': ['AB12', 'CD34'],
        'TOTAL_TAP_IN_VOLUME': [1234, 5678],
        'TOTAL_TAP_OUT_VOLUME': [1234, 5678]
    })
    mock_read_csv.return_value = mock_df

    # call function
    import_pv_train()

    # assertion
    mock_datapipe.assert_called_once_with(
        hostname=mock_env['DB_HOST'],
        username=mock_env['DB_USER'],
        password=mock_env['DB_PASS'],
        database=mock_env['DB_NAME']
    )
    mock_instance.load_db.assert_called_once()
    args, kwargs = mock_datapipe.call_args
    assert set(kwargs.values()) == {'127.0.0.1', 
                                    'test_username', 
                                    'test_password', 
                                    'test_database'}

    transformed_YEAR_MONTH = mock_df['YEAR_MONTH']
    assert transformed_YEAR_MONTH.iloc[0] == '2025-01-01'
    assert transformed_YEAR_MONTH.iloc[1] == '2024-12-01'
    mock_read_csv.assert_called_once()


@patch('loaders.import_pv_train.config_pv_train', {
    'backfill': True,
    'csv_prefix': '/tmp/incoming/pv_train/csv',
    'csv_name': 'csv_name',
    'yyyymm': '202501',
    'delimiter': ',',
    'col_pd': {
        'YEAR_MONTH': 'str',
        'DAY_TYPE': 'str',
        'TIME_PER_HOUR': 'int64',
        'PT_TYPE': 'str',
        'PT_CODE': 'str',
        'TOTAL_TAP_IN_VOLUME': 'int64',
        'TOTAL_TAP_OUT_VOLUME': 'int64'
    }
})
@patch('loaders.import_pv_train.config_db_tbl', {
    'tbl': 'r_pv_train',
    'tbl_col': {
        'YEAR_MONTH': 'year_month',
        'DAY_TYPE': 'day_type',
        'TIME_PER_HOUR': 'time_per_hour',
        'PT_TYPE': 'pt_type',
        'PT_CODE': 'pt_code',
        'TOTAL_TAP_IN_VOLUME': 'total_tap_in_volume',
        'TOTAL_TAP_OUT_VOLUME': 'total_tap_out_volume'
    }
})
@patch('loaders.import_pv_train.DataPipe')
@patch('loaders.import_pv_train.pd.read_csv')
def test_import_pv_train_backfill_T(mock_read_csv, mock_datapipe, mock_env):

    # mock datapipe
    mock_instance = MagicMock()
    mock_datapipe.return_value = mock_instance

    # mock csv data 
    mock_df = pd.DataFrame({
        'YEAR_MONTH': ['2025-01', '2024-12'],
        'DAY_TYPE': ['WEEKDAY', 'WEEKENDS/HOLIDAY'],
        'TIME_PER_HOUR': [20, 13],
        'PT_TYPE': ['TRAIN_A', 'TRAIN_B'],
        'PT_CODE': ['AB12', 'CD34'],
        'TOTAL_TAP_IN_VOLUME': [1234, 5678],
        'TOTAL_TAP_OUT_VOLUME': [1234, 5678]
    })
    mock_read_csv.return_value = mock_df

    # call function
    import_pv_train()

    # assertion
    mock_datapipe.assert_called_once_with(
        hostname=mock_env['DB_HOST'],
        username=mock_env['DB_USER'],
        password=mock_env['DB_PASS'],
        database=mock_env['DB_NAME']
    )
    mock_instance.load_db.assert_called_once()
    args, kwargs = mock_datapipe.call_args
    assert set(kwargs.values()) == {'127.0.0.1', 
                                    'test_username', 
                                    'test_password', 
                                    'test_database'}

    transformed_YEAR_MONTH = mock_df['YEAR_MONTH']
    assert transformed_YEAR_MONTH.iloc[0] == '2025-01-01'
    assert transformed_YEAR_MONTH.iloc[1] == '2024-12-01'
    mock_read_csv.assert_called_once()
