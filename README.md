# Stock Anomaly ETL Project

## Overview

**stock-anomaly** is a data engineering pipeline built with Apache Airflow that automates the extraction, transformation, and loading (ETL) of financial data—stock prices, technical indicators, and macroeconomic indicators—into PostgreSQL. The pipeline runs locally via Docker/Astro CLI and can be deployed to [Astronomer](https://www.astronomer.io/).

## Features

- **Automated ETL Pipelines**: Daily ingestion of stock data, technical indicators, and macroeconomic indicators.
- **Multiple Data Sources**:
  - Stock prices from Twelve Data and Alpha Vantage APIs
  - Technical indicators (RSI, MACD, SMA, EMA, ATR, Bollinger Bands) from Twelve Data
  - Macroeconomic indicators (GDP, Unemployment Rate, Inflation, Interest Rate, Exchange Rate, Treasury Yield) from the Federal Reserve (FRED)
- **PostgreSQL Integration**: PostgreSQL sink (local dev via Docker, production via AWS RDS).
- **Modular DAGs**: Each data domain has its own Airflow DAG for maintainability and scalability.
- **CI/CD Integration**: Automated deployment and testing via GitHub Actions and Astronomer.
- **Comprehensive Testing**: Pytest-based unit and integration tests for all DAGs and tasks.

## Architecture

- **Airflow DAGs** (in `dags/`):
  - `fetch_stock_data.py`: ETL for daily stock prices from Twelve Data
  - `fetch_time_series_data.py`: ETL for daily time series from Alpha Vantage
  - `fetch_technical_indicators.py`: ETL for technical indicators from Twelve Data
  - `fetch_macro_indicators.py`: ETL for macroeconomic indicators from FRED
- **Database**: PostgreSQL (AWS RDS in production; Dockerized Postgres for local development)
- **Orchestration**: Apache Airflow (Astronomer Runtime)
- **Testing**: Pytest (see `tests/`)

![ETL Architecture](https://github.com/NateChris14/FinanceApp/blob/main/ETL%20Pipeline%20Architecture.png)

## Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)
- [Astronomer CLI](https://docs.astronomer.io/astro/cli/install-cli) (for Astronomer deployment)

## Database (AWS RDS)

The project’s primary database is hosted on **AWS RDS for PostgreSQL** for production usage.

To run against RDS, create/configure an Airflow Connection for Postgres (`postgres_default`) and set:
- Host (RDS endpoint), Port (usually 5432)
- Database name
- Username/password

Tip: keep RDS credentials in Astronomer/Airflow secrets or environment variables rather than committing them to the repo.

## Setup & Local Development

1. **Clone the repository:**
   ```sh
   git clone https://github.com/NateChris14/FinanceApp.git
   cd FinanceApp
   ```

2. **Configure Environment Variables:**
   - Set the following Airflow Variables (via Airflow UI or CLI) for API access:
     - `TWELVEDATA_API_KEY`: Your Twelve Data API key
     - `ALPHA_VANTAGE_API_KEY`: Your Alpha Vantage API key
     - `federal_reserve_api_key`: Your FRED API key
   - Ensure Airflow Connections are set for:
     - `twelvedata_api`
     - `alpha_vantage_api`
     - `federal_reserve_api`
   - PostgreSQL connection (local):
     - `database` : postgres db name (default: postgres)
     - `user` : user name (default: postgres)
     - `password` : postgres password (default: postgres14)
     - `host` : local docker uri (local) or aws rds uri (production)
     - `port` : 5432
     - `connection type` : postgres

3. **Local PostgreSQL (optional):**
   If you don’t want to use AWS RDS for local development, start the local PostgreSQL container:

   ```sh
   docker-compose up -d
   ```
   Credentials and port are defined in docker-compose.yml. Treat these as development-only values and change them for any real deployment.

5. **Build and Start Airflow (Astronomer Runtime):**
   ```sh
   astro dev start
   ```
   This will build the Airflow image (using the provided `Dockerfile`) and start all necessary services.

6. **Access Airflow UI:**
   - Navigate to [http://localhost:8080](http://localhost:8080) in your browser.
   - Default credentials: `admin` / `admin` (unless changed).

## Configuration

- **Python Dependencies:**
  - Managed in `requirements.txt` (default: `pytest` for testing)
  - Astronomer Runtime includes all required Airflow providers
- **System Packages:**
  - None required by default (`packages.txt` is empty)
- **Database Credentials:**
  - Local dev: set in docker-compose.yml
  - Production: configure via Airflow Connection + Astronomer secrets/env vars (AWS RDS)

## Running Tests

- **Unit and Integration Tests:**
  - Located in `tests/dags/`
  - Run all tests with:
    ```sh
    pytest tests/
    ```

## Deployment

### Local Deployment
- Use Docker Compose and Astronomer CLI as described above.

### Astronomer Cloud Deployment
- This project includes a GitHub Actions workflow for CI/CD (`.github/workflows/deploy-to-astro.yml`).
- On every push to `main`, code is automatically tested and deployed to Astronomer.
- Ensure your Astronomer deployment is configured with:
  - Required API keys (as env vars / variables)
  - A PostgreSQL connection pointing to AWS RDS (host, db, user, password)

## Project Structure

```
STOCKS/
├── dags/
│   ├── fetch_macro_indicators.py
│   ├── fetch_stock_data.py
│   ├── fetch_technical_indicators.py
│   └── fetch_time_series_data.py
├── tests/
│   └── dags/
│       ├── test_fetch_macro_indicators.py
│       ├── test_fetch_stock_data.py
│       ├── test_fetch_technical_indicators.py
│       └── test_fetch_time_series_data.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── packages.txt
└── ...
```

## License

This project is intended for educational and research purposes. Please review the data provider terms of service before using in production.

---

For questions or contributions, please open an issue or pull request.
