#!/usr/bin/env python3

from unittest.mock import patch, MagicMock

from import_func import DataPipe


@patch('import_func.mysql.connector.connect')
def test_create_connection_pass(mock_connect):
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    sqlpipe = DataPipe(
        hostname='127.0.0.1',
        username='test_username',
        password='test_password',
        database='test_database',
    )
    connection = sqlpipe.create_connection()

    assert connection == mock_connection
    mock_connect.assert_called_once_with(
        host='127.0.0.1',
        user='test_username',
        password='test_password',
        database='test_database'
    )


def test_load_db_truncate_mode():
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    config_db = {
        'tbl': 'r_test',
        'tbl_col': {
            'YEAR_MONTH': 'year_month',
            'DAY_TYPE': 'day_type',
        }
    }
    data = [('2025-01-01', 'WEEKDAY')]
    sqlpipe = DataPipe(
        hostname='127.0.0.1',
        username='test_username',
        password='test_password',
        database='test_database',
    )

    with patch.object(sqlpipe, 'create_connection', return_value=mock_connection):
        sqlpipe.load_db(
            config_db=config_db,
            mode='truncate',
            timekey='year_month',
            yyyymm='202501',
            data=data
        )

    assert mock_cursor.execute.call_args_list[0].args[0] == 'TRUNCATE test_database.r_test;'
    mock_cursor.executemany.assert_called_once_with(
        'INSERT INTO test_database.r_test (`year_month`, `day_type`) VALUES (%s, %s);',
        data
    )
    assert mock_connection.commit.call_count == 1
    mock_connection.close.assert_called_once()


def test_load_db_refresh_mode():
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    config_db = {
        'tbl': 'r_test',
        'tbl_col': {
            'YEAR_MONTH': 'year_month',
            'DAY_TYPE': 'day_type',
        }
    }
    data = [('2025-01-01', 'WEEKDAY')]
    sqlpipe = DataPipe(
        hostname='127.0.0.1',
        username='test_username',
        password='test_password',
        database='test_database',
    )

    with patch.object(sqlpipe, 'create_connection', return_value=mock_connection):
        sqlpipe.load_db(
            config_db=config_db,
            mode='refresh',
            timekey='year_month',
            yyyymm='202501',
            data=data
        )

    delete_stmt = mock_cursor.execute.call_args_list[0].args[0]
    assert 'DELETE FROM test_database.r_test' in delete_stmt
    assert "`year_month` >= '2025-01-01'" in delete_stmt
    assert "`year_month` < '2025-02-01'" in delete_stmt
    mock_cursor.executemany.assert_called_once_with(
        'INSERT INTO test_database.r_test (`year_month`, `day_type`) VALUES (%s, %s);',
        data
    )
    assert mock_connection.commit.call_count == 2
    mock_connection.close.assert_called_once()


def test_load_db_append_mode():
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor

    config_db = {
        'tbl': 'r_test',
        'tbl_col': {
            'YEAR_MONTH': 'year_month',
            'DAY_TYPE': 'day_type',
        }
    }
    data = [('2025-01-01', 'WEEKDAY')]
    sqlpipe = DataPipe(
        hostname='127.0.0.1',
        username='test_username',
        password='test_password',
        database='test_database',
    )

    with patch.object(sqlpipe, 'create_connection', return_value=mock_connection):
        sqlpipe.load_db(
            config_db=config_db,
            mode='append',
            timekey='year_month',
            yyyymm='202501',
            data=data
        )

    mock_cursor.execute.assert_not_called()
    mock_cursor.executemany.assert_called_once_with(
        'INSERT INTO test_database.r_test (`year_month`, `day_type`) VALUES (%s, %s);',
        data
    )
    assert mock_connection.commit.call_count == 1
    mock_connection.close.assert_called_once()
