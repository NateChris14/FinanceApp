import pytest #test runner
from unittest.mock import patch, MagicMock #mocking external API,DB
from airflow.models import DagBag, Connection #DagBag lets us load and inspect DAGs
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

#Import the actual functions and constants from the Airflow DAG file
from dags.fetch_stock_data import (
    extract_stock_data,
    transform_stock_data,
    load_stock_data,
    TICKERS,
    POSTGRES_CONN_ID,
    API_CONN_ID
)

#DAG Tests

#Reusable test object that loads the DAGs
@pytest.fixture
def dagbag():
    """Create a DagBag object for testing"""
    return DagBag(dag_folder='dags', include_examples=False)

def test_dag_loaded(dagbag):
    """Test that the DAG is loaded correctly"""
    dag = dagbag.dags.get('stock_data_etl')
    assert dag is not None
    assert len(dag.tasks) == 3

def test_dag_structure(dagbag):
    """Test the DAG structure"""
    dag = dagbag.dags.get('stock_data_etl')
    assert dag.schedule_interval == '@daily'
    assert not dag.catchup
    assert dag.default_args['owner'] == 'airflow'

def test_dag_tasks(dagbag):
    """Test that the DAG has the correct tasks"""
    dag = dagbag.dags.get('stock_data_etl')
    tasks = dag.tasks
    task_ids = [task.task_id for task in tasks]

    assert 'extract_stock_data' in task_ids
    assert 'transform_stock_data' in task_ids
    assert 'load_stock_data' in task_ids

@patch('airflow.models.Variable.get')
@patch('airflow.providers.http.hooks.http.HttpHook.run')
@patch('airflow.models.Connection.get_connection_from_secrets')
def test_extract_stock_data_function(mock_get_conn, mock_http_run, mock_variable_get):
    """Test the extract_stock_data task"""
    #Mock the connection
    mock_conn = MagicMock()
    mock_conn.host = 'api.example.com'
    mock_conn.schema = 'https'
    mock_conn.login = 'test_user'
    mock_conn.password = 'test_pass'
    mock_conn.port = None
    mock_get_conn.return_value = mock_conn

    #Mock the API key
    mock_variable_get.return_value = 'test_api_key'

    #Mock the HTTP response
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
    mock_http_run.return_value = mock_response

    #Execute the task function directly
    result = extract_stock_data.__wrapped__()

    #Verify the results
    assert isinstance(result, dict)
    assert 'AAPL' in result
    assert '2024-01-01' in result['AAPL']

def test_transform_stock_data_function():
    """Test the transform_stock_data task"""
    #Sample input data
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

    #Execute the task function directly
    result = transform_stock_data.__wrapped__(input_data)

    #Verify the results
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]['ticker'] == 'AAPL'
    assert result[0]['date'] == '2024-01-01'
    assert result[0]['open'] == 100.00
    assert result[0]['high'] == 101.00
    assert result[0]['low'] == 99.00
    assert result[0]['close'] == 100.50
    assert result[0]['volume'] == 1000000

@patch('airflow.providers.postgres.hooks.postgres.PostgresHook.get_conn')
@patch('airflow.models.Connection.get_connection_from_secrets')
def test_load_stock_data_function(mock_get_conn, mock_postgres_get_conn):
    """Test the load_stock_data task"""
    #Mock the connection
    mock_conn = MagicMock()
    mock_conn.conn_type = 'postgres'
    mock_conn.host = 'localhost'
    mock_conn.schema = 'airflow'
    mock_conn.login = 'postgres'
    mock_conn.password = 'postgres'
    mock_conn.port = 5432
    mock_conn.extra = '{}'
    mock_get_conn.return_value = mock_conn

    #Mock the database connection and cursor
    mock_db_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_db_conn.cursor.return_value = mock_cursor
    mock_postgres_get_conn.return_value = mock_db_conn

    #Sample Input Data
    input_data = [{
        'ticker': 'AAPL',
        'date': '2024-01-01',
        'open': 100.00,
        'high': 101.00,
        'low': 99.00,
        'close': 100.50,
        'volume': 1000000
    }]

    #Execute the task function directly
    load_stock_data.__wrapped__(input_data)

    #Verify that the cursor executed the correct SQL statements
    assert mock_cursor.execute.call_count >= 2 #At least create table and insert
    mock_db_conn.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_db_conn.close.assert_called_once()

def test_dag_task_dependencies(dagbag):
    """Test that the tasks are properly connected"""
    dag = dagbag.dags.get('stock_data_etl')

    #Get the tasks
    extract_task = dag.get_task('extract_stock_data')
    transform_task = dag.get_task('transform_stock_data')
    load_task = dag.get_task('load_stock_data')

    #Check dependencies
    assert extract_task.downstream_list == [transform_task]
    assert transform_task.downstream_list == [load_task]
    assert load_task.downstream_list == []






