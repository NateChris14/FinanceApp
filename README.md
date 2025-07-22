# Stock Anomaly ETL Project

## Overview

This project, **stock-anomaly**, is a data engineering pipeline built with Apache Airflow. It automates the extraction, transformation, and loading (ETL) of financial data, including stock prices, technical indicators, and macroeconomic indicators, into a PostgreSQL database. The project is designed for robust, production-grade data workflows and is deployable both locally (via Docker Compose) and on [Astronomer](https://www.astronomer.io/).

## Features

- **Automated ETL Pipelines**: Daily ingestion of stock data, technical indicators, and macroeconomic indicators.
- **Multiple Data Sources**:
  - Stock prices from Twelve Data and Alpha Vantage APIs
  - Technical indicators (RSI, MACD, SMA, EMA, ATR, Bollinger Bands) from Twelve Data
  - Macroeconomic indicators (GDP, Unemployment Rate, Inflation, Interest Rate, Exchange Rate, Treasury Yield) from the Federal Reserve (FRED)
- **PostgreSQL Integration**: All data is loaded into a managed PostgreSQL instance.
- **Modular DAGs**: Each data domain has its own Airflow DAG for maintainability and scalability.
- **CI/CD Integration**: Automated deployment and testing via GitHub Actions and Astronomer.
- **Comprehensive Testing**: Pytest-based unit and integration tests for all DAGs and tasks.

## Architecture

- **Airflow DAGs** (in `dags/`):
  - `fetch_stock_data.py`: ETL for daily stock prices from Twelve Data
  - `fetch_time_series_data.py`: ETL for daily time series from Alpha Vantage
  - `fetch_technical_indicators.py`: ETL for technical indicators from Twelve Data
  - `fetch_macro_indicators.py`: ETL for macroeconomic indicators from FRED
- **Database**: PostgreSQL (Dockerized)
- **Orchestration**: Apache Airflow (Astronomer Runtime)
- **Testing**: Pytest (see `tests/`)

## Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/)
- [Astronomer CLI](https://docs.astronomer.io/astro/cli/install-cli) (for Astronomer deployment)

## Setup & Local Development

1. **Clone the repository:**
   ```sh
   git clone <your-repo-url>
   cd <repo-directory>
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

3. **Start PostgreSQL (Docker Compose):**
   ```sh
   docker-compose up -d
   ```
   This will start a local PostgreSQL instance on port 5432 with default credentials (`postgres`/`postgres14`).

4. **Build and Start Airflow (Astronomer Runtime):**
   ```sh
   astro dev start
   ```
   This will build the Airflow image (using the provided `Dockerfile`) and start all necessary services.

5. **Access Airflow UI:**
   - Navigate to [http://localhost:8080](http://localhost:8080) in your browser.
   - Default credentials: `admin` / `admin` (unless changed).

## Configuration

- **Python Dependencies:**
  - Managed in `requirements.txt` (default: `pytest` for testing)
  - Astronomer Runtime includes all required Airflow providers
- **System Packages:**
  - None required by default (`packages.txt` is empty)
- **Database Credentials:**
  - Set in `docker-compose.yml` (can be customized as needed)

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
- Ensure your Astronomer deployment is configured with the required API keys and connections.

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
