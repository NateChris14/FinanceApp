from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json
import requests
import time
from airflow.models import Variable

# Configuration
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'federal_reserve_api'

# List of macro indicators to fetch
MACRO_INDICATORS = ['GDP', 'Unemployment Rate', 'Inflation Rate', 'Interest Rate', 'Exchange Rate']

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id = 'macro_indicators_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False

) as dag:
    
    @task()
    def extract_macro_indicators():
        """Extract macro indicators from Federal Reserve API"""
        api_key = Variable.get('federal_reserve_api_key')

        # Initialize the HTTP hook
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        # Initialize the dictionary to store all macro indicators
        all_macro_indicators = {}

        # Map indicators to their FRED series IDs
        indicator_mapping = {
            'GDP': 'GDP',  # Gross Domestic Product
            'Unemployment Rate': 'UNRATE',  # Unemployment Rate
            'Inflation Rate': 'CPIAUCSL',  # Consumer Price Index
            'Interest Rate': 'FEDFUNDS',  # Federal Funds Rate
            'Exchange Rate': 'DEXUSEU'  # USD/EUR Exchange Rate
        }

        for indicator, series_id in indicator_mapping.items():
            endpoint = f'/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json'
            response = http_hook.run(endpoint)

            if response.status_code == 200:
                data = response.json()
                if 'observations' in data:
                    all_macro_indicators[indicator] = data['observations']
                else:
                    raise Exception(f'Unexpected response format for {indicator}')
            else:
                raise Exception(f'Failed to fetch data for {indicator}: {response.status_code}')
            
        return all_macro_indicators
    
    @task()
    def transform_macro_indicators(macro_indicators):
        """Transform the extracted macro indicators"""
        transformed_data = []

        for indicator, data in macro_indicators.items():
            for item in data:
                # Skip records with missing values ('.')
                if item['value'] == '.':
                    continue
                    
                transformed_record = {
                    'indicator': indicator,
                    'date': item['date'],
                    'value': float(item['value']),
                }
                transformed_data.append(transformed_record)
        
        return transformed_data
    
    @task()
    def load_macro_indicators(transformed_data):
        """Load transformed data into PostgreSQL"""

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS macro_indicators(
            indicator VARCHAR(50),
            date DATE,
            value DECIMAL(10,2),
            PRIMARY KEY(indicator, date)               
        );""")

        # Insert transformed data into the table
        for record in transformed_data:
            cursor.execute("""
            INSERT INTO macro_indicators (indicator, date, value)
            VALUES (%s, %s, %s)
            ON CONFLICT (indicator, date) DO UPDATE SET
            value = EXCLUDED.value
            """, (
                record['indicator'],
                record['date'],
                record['value']
            ))

        conn.commit()
        cursor.close()
        conn.close()

    # DAG Workflow - ETL Pipeline
    macro_indicators = extract_macro_indicators()
    transformed_data = transform_macro_indicators(macro_indicators)
    load_macro_indicators(transformed_data)
