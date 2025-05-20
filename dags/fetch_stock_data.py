from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
import json

# Configuration
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'twelvedata_api'  # Make sure this Airflow HTTP connection exists

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
    dag_id='stock_data_etl_twelvedata',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    @task()
    def extract_stock_data():
        """Extract stock data from Twelve Data API."""
        api_key = Variable.get("TWELVEDATA_API_KEY")
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        all_stock_data = {}

        for ticker in TICKERS:
            endpoint = f"/time_series?symbol={ticker}&interval=1day&apikey={api_key}&outputsize=100"

            response = http_hook.run(endpoint)
            if response.status_code == 200:
                data = response.json()
                if "values" in data:
                    all_stock_data[ticker] = data["values"]
                else:
                    raise Exception(f"Invalid data for {ticker}: {data}")
            else:
                raise Exception(f"Failed to fetch {ticker}: {response.status_code}")

        return all_stock_data

    @task()
    def transform_stock_data(stock_data):
        """Transform Twelve Data stock data to a flat list."""
        transformed_data = []

        for ticker, values in stock_data.items():
            for entry in values:
                transformed_record = {
                    'ticker': ticker,
                    'date': entry['datetime'].split(' ')[0],
                    'open': float(entry['open']),
                    'high': float(entry['high']),
                    'low': float(entry['low']),
                    'close': float(entry['close']),
                    'volume': int(entry['volume'])
                }
                transformed_data.append(transformed_record)

        return transformed_data

    @task()
    def load_stock_data(transformed_data):
        """Load transformed data into PostgreSQL."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_data (
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

        # Insert/update data
        for record in transformed_data:
            cursor.execute("""
            INSERT INTO stock_data (ticker, date, open, high, low, close, volume)
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

    # DAG Workflow
    stock_data = extract_stock_data()
    transformed_data = transform_stock_data(stock_data)
    load_stock_data(transformed_data)
