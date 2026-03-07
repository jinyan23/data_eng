#!/usr/bin/env python3

from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from pv_bus.import_pv_bus import import_pv_bus


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


@patch('pv_bus.import_pv_bus.config_pv', {
    'backfill': False,
    'csv_prefix': 'incoming/pv_bus/csv',
    'csv_name': 'csv_name',
    'yyyymm': '202501',
    'delimiter': ',',
    'mode': 'refresh',
    'timekey': 'year_month',
    'col_pd': {
        'YEAR_MONTH': 'str',
        'DAY_TYPE': 'str',
        'TIME_PER_HOUR': 'str',
        'PT_TYPE': 'str',
        'PT_CODE': 'str',
        'TOTAL_TAP_IN_VOLUME': 'int64',
        'TOTAL_TAP_OUT_VOLUME': 'int64'
    }
})
@patch('pv_bus.import_pv_bus.config_db_tbl', {
    'tbl': 'r_pv_bus',
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
@patch('pv_bus.import_pv_bus.DataPipe')
@patch('pv_bus.import_pv_bus.util_s3.read_csv_s3')
def test_import_pv_bus_backfill_false(mock_read_csv_s3, mock_datapipe, mock_env):
    mock_instance = MagicMock()
    mock_datapipe.return_value = mock_instance

    yyyymm = dt.strftime(dt.now() - relativedelta(months=1), '%Y%m')
    mock_df = pd.DataFrame({
        'YEAR_MONTH': ['2025-12', '2025-12'],
        'DAY_TYPE': ['WEEKDAY', 'WEEKENDS/HOLIDAY'],
        'TIME_PER_HOUR': ['20', '   '],
        'PT_TYPE': ['BUS_A', 'BUS_B'],
        'PT_CODE': ['AB12', 'CD34'],
        'TOTAL_TAP_IN_VOLUME': [1234, 5678],
        'TOTAL_TAP_OUT_VOLUME': [1234, np.nan]
    })
    mock_read_csv_s3.return_value = mock_df

    import_pv_bus()

    mock_datapipe.assert_called_once_with(
        hostname=mock_env['DB_HOST'],
        username=mock_env['DB_USER'],
        password=mock_env['DB_PASS'],
        database=mock_env['DB_NAME'],
        port=mock_env['DB_PORT'],
    )
    mock_instance.load_db.assert_called_once_with(
        config_db={
            'tbl': 'r_pv_bus',
            'tbl_col': {
                'YEAR_MONTH': 'year_month',
                'DAY_TYPE': 'day_type',
                'TIME_PER_HOUR': 'time_per_hour',
                'PT_TYPE': 'pt_type',
                'PT_CODE': 'pt_code',
                'TOTAL_TAP_IN_VOLUME': 'total_tap_in_volume',
                'TOTAL_TAP_OUT_VOLUME': 'total_tap_out_volume'
            }
        },
        mode='refresh',
        timekey='year_month',
        yyyymm=yyyymm,
        data=[
            ('2025-12-01', 'WEEKDAY', '20', 'BUS_A', 'AB12', 1234, 1234.0),
            ('2025-12-01', 'WEEKENDS/HOLIDAY', None, 'BUS_B', 'CD34', 5678, None),
        ]
    )
    mock_read_csv_s3.assert_called_once_with(
        f'incoming/pv_bus/csv/csv_name_{yyyymm}.csv',
        delimiter=',',
        dtype={
            'YEAR_MONTH': 'str',
            'DAY_TYPE': 'str',
            'TIME_PER_HOUR': 'str',
            'PT_TYPE': 'str',
            'PT_CODE': 'str',
            'TOTAL_TAP_IN_VOLUME': 'int64',
            'TOTAL_TAP_OUT_VOLUME': 'int64'
        },
        keep_default_na=False
    )


@patch('pv_bus.import_pv_bus.config_pv', {
    'backfill': True,
    'csv_prefix': 'incoming/pv_bus/csv',
    'csv_name': 'csv_name',
    'yyyymm': '202501',
    'delimiter': ',',
    'mode': 'refresh',
    'timekey': 'year_month',
    'col_pd': {
        'YEAR_MONTH': 'str',
        'DAY_TYPE': 'str',
        'TIME_PER_HOUR': 'str',
        'PT_TYPE': 'str',
        'PT_CODE': 'str',
        'TOTAL_TAP_IN_VOLUME': 'int64',
        'TOTAL_TAP_OUT_VOLUME': 'int64'
    }
})
@patch('pv_bus.import_pv_bus.config_db_tbl', {
    'tbl': 'r_pv_bus',
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
@patch('pv_bus.import_pv_bus.DataPipe')
@patch('pv_bus.import_pv_bus.util_s3.read_csv_s3')
def test_import_pv_bus_backfill_true(mock_read_csv_s3, mock_datapipe, mock_env):
    mock_instance = MagicMock()
    mock_datapipe.return_value = mock_instance

    mock_df = pd.DataFrame({
        'YEAR_MONTH': ['2025-01', '2024-12'],
        'DAY_TYPE': ['WEEKDAY', 'WEEKENDS/HOLIDAY'],
        'TIME_PER_HOUR': ['20', '13'],
        'PT_TYPE': ['BUS_A', 'BUS_B'],
        'PT_CODE': ['AB12', 'CD34'],
        'TOTAL_TAP_IN_VOLUME': [1234, 5678],
        'TOTAL_TAP_OUT_VOLUME': [1234, 5678]
    })
    mock_read_csv_s3.return_value = mock_df

    import_pv_bus()

    mock_datapipe.assert_called_once_with(
        hostname=mock_env['DB_HOST'],
        username=mock_env['DB_USER'],
        password=mock_env['DB_PASS'],
        database=mock_env['DB_NAME'],
        port=mock_env['DB_PORT'],
    )
    mock_instance.load_db.assert_called_once_with(
        config_db={
            'tbl': 'r_pv_bus',
            'tbl_col': {
                'YEAR_MONTH': 'year_month',
                'DAY_TYPE': 'day_type',
                'TIME_PER_HOUR': 'time_per_hour',
                'PT_TYPE': 'pt_type',
                'PT_CODE': 'pt_code',
                'TOTAL_TAP_IN_VOLUME': 'total_tap_in_volume',
                'TOTAL_TAP_OUT_VOLUME': 'total_tap_out_volume'
            }
        },
        mode='refresh',
        timekey='year_month',
        yyyymm='202501',
        data=[
            ('2025-01-01', 'WEEKDAY', '20', 'BUS_A', 'AB12', 1234, 1234),
            ('2024-12-01', 'WEEKENDS/HOLIDAY', '13', 'BUS_B', 'CD34', 5678, 5678),
        ]
    )
    mock_read_csv_s3.assert_called_once_with(
        'incoming/pv_bus/csv/csv_name_202501.csv',
        delimiter=',',
        dtype={
            'YEAR_MONTH': 'str',
            'DAY_TYPE': 'str',
            'TIME_PER_HOUR': 'str',
            'PT_TYPE': 'str',
            'PT_CODE': 'str',
            'TOTAL_TAP_IN_VOLUME': 'int64',
            'TOTAL_TAP_OUT_VOLUME': 'int64'
        },
        keep_default_na=False
    )
