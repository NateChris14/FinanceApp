import pytest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag, Connection
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

# Import the actual functions and constants from the Airflow DAG file
from dags.fetch_macro_indicators import (
    extract_macro_indicators,
    transform_macro_indicators,
    load_macro_indicators,
    MACRO_INDICATORS,
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
    dag = dagbag.dags.get('macro_indicators_etl')
    assert dag is not None
    assert len(dag.tasks) == 3

def test_dag_structure(dagbag):
    """Test the DAG structure"""
    dag = dagbag.dags.get('macro_indicators_etl')
    assert dag.schedule_interval == '@daily'
    assert not dag.catchup
    assert dag.default_args['owner'] == 'airflow'

def test_dag_tasks(dagbag):
    """Test that the DAG has the correct tasks"""
    dag = dagbag.dags.get('macro_indicators_etl')
    tasks = dag.tasks
    task_ids = [task.task_id for task in tasks]

    assert 'extract_macro_indicators' in task_ids
    assert 'transform_macro_indicators' in task_ids
    assert 'load_macro_indicators' in task_ids

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
            'observations': [
                {
                    'realtime_start': '2024-01-01',
                    'realtime_end': '2024-01-01',
                    'date': '2024-01-01',
                    'value': '100.50'
                }
            ]
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

def test_extract_macro_indicators(mock_airflow_conn, mock_http_hook, mock_variable):
    """Test the extract_macro_indicators task"""
    result = extract_macro_indicators.__wrapped__()

    assert isinstance(result, dict)
    expected_indicators = [
        'GDP',
        'Unemployment Rate',
        'Inflation Rate',
        'Interest Rate',
        'Exchange Rate',
        '10-Year Treasury Yield'
    ]
    for indicator in expected_indicators:
        assert indicator in result
        assert isinstance(result[indicator], list)
        assert len(result[indicator]) > 0
        assert 'date' in result[indicator][0]
        assert 'value' in result[indicator][0]
        assert 'realtime_start' in result[indicator][0]
        assert 'realtime_end' in result[indicator][0]

def test_transform_macro_indicators():
    """Test the transform_macro_indicators task"""
    input_data = {
        'GDP': [
            {
                'date': '2024-01-01',
                'value': '100.50'
            }
        ],
        '10-Year Treasury Yield': [
            {
                'date': '2024-01-01',
                'value': '4.25'
            }
        ]
    }

    result = transform_macro_indicators.__wrapped__(input_data)

    assert isinstance(result, list)
    assert len(result) == 2  # One record for each indicator
    
    # Check GDP record
    gdp_record = next(r for r in result if r['indicator'] == 'GDP')
    assert gdp_record['date'] == '2024-01-01'
    assert gdp_record['value'] == 100.50
    
    # Check Treasury Yield record
    treasury_record = next(r for r in result if r['indicator'] == '10-Year Treasury Yield')
    assert treasury_record['date'] == '2024-01-01'
    assert treasury_record['value'] == 4.25

def test_load_macro_indicators(mock_postgres_hook):
    """Test the load_macro_indicators task"""
    input_data = [{
        'indicator': 'GDP',
        'date': '2024-01-01',
        'value': 100.50
    }]

    load_macro_indicators.__wrapped__(input_data)

    # Get the mock cursor from the mock connection
    mock_cursor = mock_postgres_hook.return_value.cursor.return_value
    
    # Verify SQL operations
    assert mock_cursor.execute.call_count >= 2  # At least create table and insert
    mock_postgres_hook.return_value.commit.assert_called_once()
    mock_cursor.close.assert_called_once()
    mock_postgres_hook.return_value.close.assert_called_once()

    # Verify the table creation SQL
    create_table_call = mock_cursor.execute.call_args_list[0]
    assert 'CREATE TABLE IF NOT EXISTS macro_indicators' in create_table_call[0][0]
    assert 'indicator VARCHAR(50)' in create_table_call[0][0]
    assert 'date DATE' in create_table_call[0][0]
    assert 'value DECIMAL(10,2)' in create_table_call[0][0]
    assert 'PRIMARY KEY(indicator, date)' in create_table_call[0][0]

    # Verify the insert SQL
    insert_call = mock_cursor.execute.call_args_list[1]
    assert 'INSERT INTO macro_indicators' in insert_call[0][0]
    assert 'ON CONFLICT (indicator, date) DO UPDATE SET' in insert_call[0][0]

def test_dag_task_dependencies(dagbag):
    """Test that the tasks are properly connected"""
    dag = dagbag.dags.get('macro_indicators_etl')

    # Get the tasks
    extract_task = dag.get_task('extract_macro_indicators')
    transform_task = dag.get_task('transform_macro_indicators')
    load_task = dag.get_task('load_macro_indicators')

    # Check dependencies
    assert extract_task.downstream_list == [transform_task]
    assert transform_task.downstream_list == [load_task]
    assert load_task.downstream_list == [] 