import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from airflow.models import DagBag, Connection
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dags.fetch_technical_indicators import (
    extract_technical_indicators,
    transform_technical_indicators,
    load_technical_indicators,
    TICKERS,
    POSTGRES_CONN_ID,
    API_CONN_ID
)

# Mock data for testing
MOCK_RSI_RESPONSE = {
    'values': [
        {
            'datetime': '2024-03-20',
            'rsi': '65.4321'
        },
        {
            'datetime': '2024-03-19',
            'rsi': '62.1234'
        }
    ]
}

MOCK_MACD_RESPONSE = {
    'values': [
        {
            'datetime': '2024-03-20',
            'macd': '1.2345'
        },
        {
            'datetime': '2024-03-19',
            'macd': '1.1234'
        }
    ]
}

MOCK_SMA_RESPONSE = {
    'values': [
        {
            'datetime': '2024-03-20',
            'sma': '150.1234'
        },
        {
            'datetime': '2024-03-19',
            'sma': '149.8765'
        }
    ]
}

MOCK_EMA_RESPONSE = {
    'values': [
        {
            'datetime': '2024-03-20',
            'ema': '150.2345'
        },
        {
            'datetime': '2024-03-19',
            'ema': '149.9876'
        }
    ]
}

MOCK_ATR_RESPONSE = {
    'values': [
        {
            'datetime': '2024-03-20',
            'atr': '2.3456'
        },
        {
            'datetime': '2024-03-19',
            'atr': '2.2345'
        }
    ]
}

MOCK_BBANDS_RESPONSE = {
    'values': [
        {
            'datetime': '2024-03-20',
            'upper_band': '155.1234',
            'middle_band': '150.1234',
            'lower_band': '145.1234'
        },
        {
            'datetime': '2024-03-19',
            'upper_band': '154.8765',
            'middle_band': '149.8765',
            'lower_band': '144.8765'
        }
    ]
}

@pytest.fixture
def dagbag():
    """Create a DagBag object for testing"""
    return DagBag(dag_folder='dags', include_examples=False)

def test_dag_loaded(dagbag):
    """Test that the DAG is loaded correctly"""
    dag = dagbag.dags.get('technical_indicators_etl')
    assert dag is not None
    assert len(dag.tasks) == 3

def test_dag_structure(dagbag):
    """Test the DAG structure"""
    dag = dagbag.dags.get('technical_indicators_etl')
    assert dag.schedule_interval == '@daily'
    assert not dag.catchup
    assert dag.default_args['owner'] == 'airflow'

def test_dag_tasks(dagbag):
    """Test that the DAG has the correct tasks"""
    dag = dagbag.dags.get('technical_indicators_etl')
    tasks = dag.tasks
    task_ids = [task.task_id for task in tasks]

    assert 'extract_technical_indicators' in task_ids
    assert 'transform_technical_indicators' in task_ids
    assert 'load_technical_indicators' in task_ids

@patch('airflow.models.Variable.get')
@patch('airflow.providers.http.hooks.http.HttpHook.run')
@patch('airflow.models.Connection.get_connection_from_secrets')
def test_extract_technical_indicators_function(mock_get_conn, mock_http_run, mock_variable_get):
    """Test the extract_technical_indicators task"""
    # Mock the connection
    mock_conn = MagicMock()
    mock_conn.host = 'api.twelvedata.com'
    mock_conn.schema = 'https'
    mock_conn.login = 'test_user'
    mock_conn.password = 'test_pass'
    mock_conn.port = None
    mock_get_conn.return_value = mock_conn

    # Mock the API key
    mock_variable_get.return_value = 'test_api_key'

    # Mock the HTTP responses
    mock_http_run.side_effect = [
        MagicMock(status_code=200, json=lambda: MOCK_RSI_RESPONSE),
        MagicMock(status_code=200, json=lambda: MOCK_MACD_RESPONSE),
        MagicMock(status_code=200, json=lambda: MOCK_SMA_RESPONSE),
        MagicMock(status_code=200, json=lambda: MOCK_EMA_RESPONSE),
        MagicMock(status_code=200, json=lambda: MOCK_ATR_RESPONSE),
        MagicMock(status_code=200, json=lambda: MOCK_BBANDS_RESPONSE)
    ]

    # Execute the task function directly
    result = extract_technical_indicators.__wrapped__()

    # Verify the results
    assert isinstance(result, dict)
    assert 'AAPL' in result
    assert all(key in result['AAPL'] for key in ['RSI', 'MACD', 'SMA', 'EMA', 'ATR', 'BBANDS'])
    assert isinstance(result['AAPL']['RSI'], list)
    assert isinstance(result['AAPL']['MACD'], list)
    assert isinstance(result['AAPL']['SMA'], list)
    assert isinstance(result['AAPL']['EMA'], list)
    assert isinstance(result['AAPL']['ATR'], list)
    assert isinstance(result['AAPL']['BBANDS'], list)

def test_transform_technical_indicators_function():
    """Test the transform_technical_indicators task"""
    # Sample input data
    input_data = {
        'AAPL': {
            'RSI': [
                {
                    'datetime': '2024-03-20',
                    'rsi': '65.4321'
                },
                {
                    'datetime': '2024-03-19',
                    'rsi': '62.1234'
                }
            ],
            'MACD': [
                {
                    'datetime': '2024-03-20',
                    'macd': '1.2345'
                },
                {
                    'datetime': '2024-03-19',
                    'macd': '1.1234'
                }
            ],
            'SMA': [
                {
                    'datetime': '2024-03-20',
                    'sma': '150.1234'
                },
                {
                    'datetime': '2024-03-19',
                    'sma': '149.8765'
                }
            ],
            'EMA': [
                {
                    'datetime': '2024-03-20',
                    'ema': '150.2345'
                },
                {
                    'datetime': '2024-03-19',
                    'ema': '149.9876'
                }
            ],
            'ATR': [
                {
                    'datetime': '2024-03-20',
                    'atr': '2.3456'
                },
                {
                    'datetime': '2024-03-19',
                    'atr': '2.2345'
                }
            ],
            'BBANDS': [
                {
                    'datetime': '2024-03-20',
                    'upper_band': '155.1234',
                    'middle_band': '150.1234',
                    'lower_band': '145.1234'
                },
                {
                    'datetime': '2024-03-19',
                    'upper_band': '154.8765',
                    'middle_band': '149.8765',
                    'lower_band': '144.8765'
                }
            ]
        }
    }

    # Execute the task function directly
    result = transform_technical_indicators.__wrapped__(input_data)

    # Verify the results
    assert isinstance(result, list)
    assert len(result) == 2  # Two dates
    assert all(isinstance(record['rsi'], float) for record in result)
    assert all(isinstance(record['macd'], float) for record in result)
    assert all(isinstance(record['sma'], float) for record in result)
    assert all(isinstance(record['ema'], float) for record in result)
    assert all(isinstance(record['atr'], float) for record in result)
    assert all(isinstance(record['bb_upper'], float) for record in result)
    assert all(isinstance(record['bb_middle'], float) for record in result)
    assert all(isinstance(record['bb_lower'], float) for record in result)
    assert all('ticker' in record for record in result)
    assert all('date' in record for record in result)
    
    # Verify specific values for 2024-03-20
    date_2024_03_20 = next(record for record in result if record['date'] == '2024-03-20')
    assert date_2024_03_20['rsi'] == 65.4321
    assert date_2024_03_20['macd'] == 1.2345
    assert date_2024_03_20['sma'] == 150.1234
    assert date_2024_03_20['ema'] == 150.2345
    assert date_2024_03_20['atr'] == 2.3456
    assert date_2024_03_20['bb_upper'] == 155.1234
    assert date_2024_03_20['bb_middle'] == 150.1234
    assert date_2024_03_20['bb_lower'] == 145.1234

@patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn')
@patch('airflow.models.Connection.get_connection_from_secrets')
def test_load_technical_indicators_function(mock_get_conn, mock_postgres_get_conn):
    """Test the load_technical_indicators task"""
    # Mock the connection
    mock_conn = MagicMock()
    mock_conn.conn_type = 'postgres'
    mock_conn.host = 'localhost'
    mock_conn.schema = 'airflow'
    mock_conn.login = 'postgres'
    mock_conn.password = 'postgres'
    mock_conn.port = 5432
    mock_conn.extra = '{}'
    mock_get_conn.return_value = mock_conn

    # Mock the database connection and cursor
    mock_db_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_db_conn.cursor.return_value = mock_cursor
    mock_postgres_get_conn.return_value = mock_db_conn

    # Sample input data
    input_data = [{
        'ticker': 'AAPL',
        'date': '2024-03-20',
        'rsi': 65.4321,
        'macd': 1.2345,
        'sma': 150.1234,
        'ema': 150.2345,
        'atr': 2.3456,
        'bb_upper': 155.1234,
        'bb_middle': 150.1234,
        'bb_lower': 145.1234
    }]

    # Execute the task function directly
    load_technical_indicators.__wrapped__(input_data)

    # Verify that the cursor executed the correct SQL statements
    assert mock_cursor.execute.call_count >= 2  # At least create table and insert
    mock_db_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_db_conn.close.assert_called_once()

def test_dag_task_dependencies(dagbag):
    """Test that the tasks are properly connected"""
    dag = dagbag.dags.get('technical_indicators_etl')

    # Get the tasks
    extract_task = dag.get_task('extract_technical_indicators')
    transform_task = dag.get_task('transform_technical_indicators')
    load_task = dag.get_task('load_technical_indicators')

    # Check dependencies
    assert extract_task.downstream_list == [transform_task]
    assert transform_task.downstream_list == [load_task]
    assert load_task.downstream_list == [] 