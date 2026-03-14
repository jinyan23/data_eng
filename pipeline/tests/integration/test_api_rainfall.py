#!/usr/bin/env python3

from unittest.mock import MagicMock, patch

from rainfall.api_rainfall import api_rainfall


@patch('rainfall.api_rainfall.conf', {
    'config_proc': {
        'url': 'https://example.com/rainfall',
    }
})
@patch('rainfall.api_rainfall.util_s3.write_csv_s3')
@patch('rainfall.api_rainfall.requests.get')
def test_api_rainfall_writes_csv(mock_get, mock_write, monkeypatch):
    monkeypatch.setenv('NEA_KEY', 'test-key')
    monkeypatch.setenv('BUCKET', 'test-bucket')

    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        'data': {
            'stations': [
                {
                    'id': 'S1',
                    'deviceId': 'S1',
                    'name': 'Station 1',
                    'location': {'latitude': 1.3, 'longitude': 103.8},
                }
            ],
            'readings': [
                {
                    'timestamp': '2026-03-14T10: 15: 00+08: 00',
                    'data': [
                        {'stationId': 'S1', 'value': 0}
                    ],
                }
            ],
        }
    }
    mock_get.return_value = mock_resp

    api_rainfall()

    mock_get.assert_called_once_with(
        'https://example.com/rainfall',
        headers={'X-Api-Key': 'test-key'},
    )
    assert mock_write.called
    args, kwargs = mock_write.call_args
    df = args[0]
    obj_key = args[1]

    assert obj_key == 'incoming/rainfall/rainfall_20260314_101500.csv'
    assert kwargs['sep'] == '|'
    assert kwargs['index'] is False

    expected_cols = {
        'timestamp',
        'stationId',
        'value',
        'id',
        'deviceId',
        'name',
        'location.latitude',
        'location.longitude',
    }
    assert expected_cols.issubset(set(df.columns))
