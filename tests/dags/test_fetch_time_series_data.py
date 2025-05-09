import pytest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag, Connection
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Import the actual functions and constants from the Airflow DAG file
from dags.fetch_time_series_data import (
    extract_time_series_data,
    transform_time_series_data,
    load_time_series_data,
    TICKERS,
    POSTGRES_CONN_ID,
    API_CONN_ID
)

# DAG Tests
@pytest.fixture
def dagbag():
    """Create a DagBag object for testing"""
    return DagBag(dag_folder='dags', include_examples=False)

def test_dag_loaded(dagbag):
    """Test that the DAG is loaded correctly"""
    dag = dagbag.dags.get('time_series_etl')
    assert dag is not None
    assert len(dag.tasks) == 3

def test_dag_structure(dagbag):
    """Test the DAG structure"""
    dag = dagbag.dags.get('time_series_etl')
    assert dag.schedule_interval == '@daily'
    assert not dag.catchup
    assert dag.default_args['owner'] == 'airflow'

def test_dag_tasks(dagbag):
    """Test that the DAG has the correct tasks"""
    dag = dagbag.dags.get('time_series_etl')
    tasks = dag.tasks
    task_ids = [task.task_id for task in tasks]

    assert 'extract_time_series_data' in task_ids
    assert 'transform_time_series_data' in task_ids
    assert 'load_time_series_data' in task_ids

@pytest.fixture
def mock_airflow_conn():
    """Mock Airflow connection"""
    with patch('airflow.models.Connection.get_connection_from_secrets') as mock:
        mock_conn = MagicMock()
        mock_conn.host = 'api.example.com'
        mock_conn.schema = 'https'
        mock_conn.login = 'test_user'
        mock_conn.password = 'test_pass'
        mock_conn.port = None
        mock.return_value = mock_conn
        yield mock

@pytest.fixture
def mock_http_hook():
    """Mock HTTP hook"""
    with patch('airflow.providers.http.hooks.http.HttpHook.run') as mock:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'Time Series (Daily)': {
                '2024-01-01': {
                    '1. open': '100.00',
                    '2. high': '101.00',
                    '3. low': '99.00',
                    '4. close': '100.50',
                    '5. volume': '1000000'
                }
            }
        }
        mock.return_value = mock_response
        yield mock

@pytest.fixture
def mock_postgres_hook():
    """Mock Postgres hook"""
    with patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn') as mock:
        mock_db_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_db_conn.cursor.return_value = mock_cursor
        mock.return_value = mock_db_conn
        yield mock

@pytest.fixture
def mock_variable():
    """Mock Airflow Variable"""
    with patch('airflow.models.Variable.get') as mock:
        mock.return_value = 'test_api_key'
        yield mock

def test_extract_time_series_data(mock_airflow_conn, mock_http_hook, mock_variable):
    """Test the extract_time_series_data task"""
    result = extract_time_series_data.__wrapped__()

    assert isinstance(result, dict)
    assert 'AAPL' in result
    assert '2024-01-01' in result['AAPL']
    assert result['AAPL']['2024-01-01']['1. open'] == '100.00'
    assert result['AAPL']['2024-01-01']['5. volume'] == '1000000'

def test_transform_time_series_data():
    """Test the transform_time_series_data task"""
    input_data = {
        'AAPL': {
            '2024-01-01': {
                '1. open': '100.00',
                '2. high': '101.00',
                '3. low': '99.00',
                '4. close': '100.50',
                '5. volume': '1000000'
            }
        }
    }

    result = transform_time_series_data.__wrapped__(input_data)

    assert isinstance(result, list)
    assert len(result) == 1
    record = result[0]
    assert record['ticker'] == 'AAPL'
    assert record['date'] == '2024-01-01'
    assert record['open'] == 100.00
    assert record['high'] == 101.00
    assert record['low'] == 99.00
    assert record['close'] == 100.50
    assert record['volume'] == 1000000

def test_load_time_series_data(mock_postgres_hook):
    """Test the load_time_series_data task"""
    input_data = [{
        'ticker': 'AAPL',
        'date': '2024-01-01',
        'open': 100.00,
        'high': 101.00,
        'low': 99.00,
        'close': 100.50,
        'volume': 1000000
    }]

    load_time_series_data.__wrapped__(input_data)

    # Get the mock cursor from the mock connection
    mock_cursor = mock_postgres_hook.return_value.cursor.return_value
    
    # Verify SQL operations
    assert mock_cursor.execute.call_count >= 2  # At least create table and insert
    mock_postgres_hook.return_value.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_postgres_hook.return_value.close.assert_called_once()

    # Verify the table creation SQL
    create_table_call = mock_cursor.execute.call_args_list[0]
    assert 'CREATE TABLE IF NOT EXISTS stock_time_series' in create_table_call[0][0]
    assert 'ticker VARCHAR(10)' in create_table_call[0][0]
    assert 'volume BIGINT' in create_table_call[0][0]
    assert 'PRIMARY KEY (ticker, date)' in create_table_call[0][0]

    # Verify the insert SQL
    insert_call = mock_cursor.execute.call_args_list[1]
    assert 'INSERT INTO stock_time_series' in insert_call[0][0]
    assert 'ON CONFLICT (ticker, date) DO UPDATE SET' in insert_call[0][0]

def test_dag_task_dependencies(dagbag):
    """Test that the tasks are properly connected"""
    dag = dagbag.dags.get('time_series_etl')

    # Get the tasks
    extract_task = dag.get_task('extract_time_series_data')
    transform_task = dag.get_task('transform_time_series_data')
    load_task = dag.get_task('load_time_series_data')

    # Check dependencies
    assert extract_task.downstream_list == [transform_task]
    assert transform_task.downstream_list == [load_task]
    assert load_task.downstream_list == [] 