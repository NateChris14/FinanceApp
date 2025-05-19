from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json
import requests
from airflow.models import Variable

# Configuration
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'alpha_vantage_api'

# List of stock tickers to fetch
TICKERS = [
    'AAPL',  # Apple Inc.
    'MSFT',  # Microsoft Corp.
    'GOOGL', # Alphabet Inc. (Google)
    'AMZN',  # Amazon.com Inc.
    'META',  # Meta Platforms Inc. (Facebook)
    'TSLA',  # Tesla Inc.
    'NVDA',  # NVIDIA Corp.
    'JPM',   # JPMorgan Chase & Co.
    'V',     # Visa Inc.
    'JNJ',   # Johnson & Johnson
    'WMT',   # Walmart Inc.
    'PG',    # Procter & Gamble
    'DIS',   # Walt Disney Co.
    'MA',    # Mastercard Inc.
    'HD',    # Home Depot
    'XOM',   # Exxon Mobil Corp.
    'PFE',   # Pfizer Inc.
    'KO',    # Coca-Cola Co.
    'PEP',   # PepsiCo Inc.
    'BAC',   # Bank of America
]


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id = 'time_series_etl',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    @task()
    def extract_time_series_data():
        """Extract time series data from Alpha Vantage API using Airflow Connection"""
        # Get API key from Airflow Variables
        api_key = Variable.get("ALPHA_VANTAGE_API_KEY")

        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        all_time_series_data = {}
        for ticker in TICKERS:
            # Build the API endpoint - using TIME_SERIES_DAILY instead of TIME_SERIES_DAILY_ADJUSTED
            endpoint = f'/query?function=TIME_SERIES_DAILY&symbol={ticker}&outputsize=full&apikey={api_key}'

            # Make the request via the HTTP Hook
            response = http_hook.run(endpoint)

            if response.status_code == 200:
                data = response.json()
                # Log the response for debugging
                print(f"API Response for {ticker}:", data)
                
                if 'Time Series (Daily)' in data:
                    all_time_series_data[ticker] = data['Time Series (Daily)']
                elif 'Error Message' in data:
                    raise Exception(f"API Error for {ticker}: {data['Error Message']}")
                elif 'Note' in data:
                    raise Exception(f"API Rate Limit for {ticker}: {data['Note']}")
                else:
                    raise Exception(f"Invalid data format for {ticker}. Response: {data}")
            else:
                raise Exception(f"Failed to fetch data for {ticker}: {response.status_code} - {response.text}")
            
        return all_time_series_data
    
    @task()
    def transform_time_series_data(time_series_data):
        """Transform the extracted time series data"""
        transformed_data = []

        for ticker, daily_data in time_series_data.items():
            for date, values in daily_data.items():
                record = {
                    'ticker': ticker,
                    'date': date,
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'volume': int(values['5. volume'])
                }
                transformed_data.append(record)
        
        return transformed_data

    @task()
    def load_time_series_data(transformed_data):
        """Load the transformed time series data into the database"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_time_series (
            ticker VARCHAR(10),
            date DATE,
            open DECIMAL(10,2),
            high DECIMAL(10,2),
            low DECIMAL(10,2),
            close DECIMAL(10,2),
            volume BIGINT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (ticker, date)
        );
        """)

        # Insert transformed data into the table
        for record in transformed_data:
            cursor.execute("""
            INSERT INTO stock_time_series 
            (ticker, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, date) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
            """, (
                record['ticker'],
                record['date'],
                record['open'],
                record['high'],
                record['low'],
                record['close'],
                record['volume']
            ))

        conn.commit()
        cursor.close()
        conn.close()

    # DAG Workflow - ETL Pipeline
    time_series_data = extract_time_series_data()
    transformed_data = transform_time_series_data(time_series_data)
    load_time_series_data(transformed_data)

    


    

