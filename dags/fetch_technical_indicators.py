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
API_CONN_ID = 'twelvedata_api'

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
    dag_id = 'technical_indicators_etl',
    default_args = default_args,
    schedule_interval = '@daily',
    catchup = False
) as dag:
    
    @task()
    def extract_technical_indicators():
        """Extract technical indicators from Twelve Data API"""
        # Get API key from Airflow Variables
        api_key = Variable.get("TWELVEDATA_API_KEY")

        http_hook = HttpHook(http_conn_id = API_CONN_ID, method = 'GET')
        
        all_technical_indicators = {}

        for ticker in TICKERS:
            # Initialize the ticker dictionary
            all_technical_indicators[ticker] = {}

            # Build the API endpoint for RSI
            endpoint_rsi = f'/rsi?symbol={ticker}&interval=1day&time_period=14&apikey={api_key}'
            response_rsi = http_hook.run(endpoint_rsi)

            if response_rsi.status_code == 200:
                rsi_json = response_rsi.json()
                if 'values' in rsi_json:
                    all_technical_indicators[ticker]['RSI'] = rsi_json['values']
                else:
                    raise Exception(f"RSI data not found for {ticker}")
            else:
                raise Exception(f"Failed to fetch RSI data for {ticker}: {response_rsi.status_code}")
            
            time.sleep(12)

            # Build the API endpoint for MACD
            endpoint_macd = f'/macd?symbol={ticker}&interval=1day&apikey={api_key}'
            response_macd = http_hook.run(endpoint_macd)

            if response_macd.status_code == 200:
                macd_json = response_macd.json()
                if 'values' in macd_json:
                    all_technical_indicators[ticker]['MACD'] = macd_json['values']
                else:
                    raise Exception(f"MACD data not found for {ticker}")
            else:
                raise Exception(f"Failed to fetch MACD data for {ticker}: {response_macd.status_code}")
            
            time.sleep(12)

            # Build the API endpoint for SMA
            endpoint_sma = f'/sma?symbol={ticker}&interval=1day&time_period=20&apikey={api_key}'
            response_sma = http_hook.run(endpoint_sma)

            if response_sma.status_code == 200:
                sma_json = response_sma.json()
                if 'values' in sma_json:
                    all_technical_indicators[ticker]['SMA'] = sma_json['values']
                else:
                    raise Exception(f"SMA data not found for {ticker}")
            else:
                raise Exception(f"Failed to fetch SMA data for {ticker}: {response_sma.status_code}")
            
            time.sleep(12)

            # Build the API endpoint for EMA
            endpoint_ema = f'/ema?symbol={ticker}&interval=1day&time_period=20&apikey={api_key}'
            response_ema = http_hook.run(endpoint_ema)

            if response_ema.status_code == 200:
                ema_json = response_ema.json()
                if 'values' in ema_json:
                    all_technical_indicators[ticker]['EMA'] = ema_json['values']
                else:
                    raise Exception(f"EMA data not found for {ticker}")
            else:
                raise Exception(f"Failed to fetch EMA data for {ticker}: {response_ema.status_code}")
            
            time.sleep(12)

            # Build the API endpoint for ATR
            endpoint_atr = f'/atr?symbol={ticker}&interval=1day&time_period=14&apikey={api_key}'
            response_atr = http_hook.run(endpoint_atr)

            if response_atr.status_code == 200:
                atr_json = response_atr.json()
                if 'values' in atr_json:
                    all_technical_indicators[ticker]['ATR'] = atr_json['values']
                else:
                    raise Exception(f"ATR data not found for {ticker}")
            else:
                raise Exception(f"Failed to fetch ATR data for {ticker}: {response_atr.status_code}")
            
            time.sleep(12)

            # Build the API endpoint for Bollinger Bands
            endpoint_bbands = f'/bbands?symbol={ticker}&interval=1day&time_period=20&apikey={api_key}'
            response_bbands = http_hook.run(endpoint_bbands)

            if response_bbands.status_code == 200:
                bbands_json = response_bbands.json()
                if 'values' in bbands_json:
                    all_technical_indicators[ticker]['BBANDS'] = bbands_json['values']
                else:
                    raise Exception(f"Bollinger Bands data not found for {ticker}")
            else:
                raise Exception(f"Failed to fetch Bollinger Bands data for {ticker}: {response_bbands.status_code}")
            
            time.sleep(12)

        return all_technical_indicators
    
    @task()
    def transform_technical_indicators(technical_indicators):
        """Transform the extracted technical indicators"""
        transformed_data = []

        for ticker, indicators in technical_indicators.items():
            # Extract dates from each indicator
            rsi_dict = {item["datetime"]: item for item in indicators.get("RSI", [])}
            macd_dict = {item["datetime"]: item for item in indicators.get("MACD", [])}
            sma_dict = {item["datetime"]: item for item in indicators.get("SMA", [])}
            ema_dict = {item["datetime"]: item for item in indicators.get("EMA", [])}
            atr_dict = {item["datetime"]: item for item in indicators.get("ATR", [])}
            bbands_dict = {item["datetime"]: item for item in indicators.get("BBANDS", [])}

            all_dates = set(rsi_dict.keys()) | set(macd_dict.keys()) | set(sma_dict.keys()) | \
                       set(ema_dict.keys()) | set(atr_dict.keys()) | set(bbands_dict.keys())

            for date in all_dates:
                record = {
                    'ticker': ticker,
                    'date': date,
                    'rsi': float(rsi_dict[date]['rsi']) if date in rsi_dict else None,
                    'macd': float(macd_dict[date]['macd']) if date in macd_dict else None,
                    'sma': float(sma_dict[date]['sma']) if date in sma_dict else None,
                    'ema': float(ema_dict[date]['ema']) if date in ema_dict else None,
                    'atr': float(atr_dict[date]['atr']) if date in atr_dict else None,
                    'bb_upper': float(bbands_dict[date]['upper_band']) if date in bbands_dict else None,
                    'bb_middle': float(bbands_dict[date]['middle_band']) if date in bbands_dict else None,
                    'bb_lower': float(bbands_dict[date]['lower_band']) if date in bbands_dict else None
                }
                transformed_data.append(record)

        return transformed_data

    
    @task()
    def load_technical_indicators(transformed_data):
        """Load the transformed technical indicators into the database"""
        pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Drop the table if it exists to ensure clean state
        cursor.execute("DROP TABLE IF EXISTS technical_indicators;")

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS technical_indicators(
            ticker VARCHAR(10),
            date DATE,
            rsi DECIMAL(10,4),
            macd DECIMAL(10,4),
            sma DECIMAL(10,4),
            ema DECIMAL(10,4),
            atr DECIMAL(10,4),
            bb_upper DECIMAL(10,4),
            bb_middle DECIMAL(10,4),
            bb_lower DECIMAL(10,4),
            PRIMARY KEY (ticker, date)           
            );""")

        # Insert transformed data into the table
        for record in transformed_data:
            cursor.execute("""
            INSERT INTO technical_indicators
            (ticker, date, rsi, macd, sma, ema, atr, bb_upper, bb_middle, bb_lower)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticker, date) DO UPDATE SET
            rsi = EXCLUDED.rsi,
            macd = EXCLUDED.macd,
            sma = EXCLUDED.sma,
            ema = EXCLUDED.ema,
            atr = EXCLUDED.atr,
            bb_upper = EXCLUDED.bb_upper,
            bb_middle = EXCLUDED.bb_middle,
            bb_lower = EXCLUDED.bb_lower;""", (
                record['ticker'],
                record['date'],
                record['rsi'],
                record['macd'],
                record['sma'],
                record['ema'],
                record['atr'],
                record['bb_upper'],
                record['bb_middle'],
                record['bb_lower']
            ))

        conn.commit()
        cursor.close()
        conn.close()

    # DAG Workflow - ETL Pipeline
    technical_indicators = extract_technical_indicators()
    transformed_data = transform_technical_indicators(technical_indicators)
    load_technical_indicators(transformed_data)                                                        


