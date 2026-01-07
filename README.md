# Crypto ETL Pipeline (Local Analytics)

A robust asynchronous **ETL (Extract, Transform, Load)** pipeline designed for crypto market analysis. This project automates the journey from raw API data to cleaned datasets and visual insights using Python and Pandas.

## ETL Architecture
The pipeline follows a strict separation of concerns to ensure scalability and maintainability:

1.  **Extract:** Asynchronously fetches historical market data (Price, Volume, Market Cap) from the CoinGecko API. Implements **Semaphore** concurrency control to stay within Rate Limits.
2.  **Transform:**
    * **Normalization:** Merges multiple API streams into a single structured DataFrame.
    * **Feature Engineering:** Calculates volatility, moving averages, and market rankings using custom Pandas-based logic.
    * **Validation:** Uses Python decorators to ensure data integrity during the analysis phase.
3.  **Load:** Persists the cleaned and transformed data into local **CSV** storage and generates automated **Matplotlib** visualizations for reporting.



## Tech Stack
* **Core:** Python 3.11
* **Data Handling:** Pandas (Transformation & Analysis)
* **Async IO:** Aiohttp, Asyncio
* **Visualization:** Matplotlib
* **Testing:** Pytest, Unittest.mock (Unit & Integration tests)

## Key Engineering Highlights
* **Decorator-Driven Analysis:** Implemented a `@get_coin_currency_pair` decorator in the `CryptoAnalyzer` to dry up code and handle data filtering centrally.
* **Resilient Ingestion:** The `BaseFetchClass` handles network timeouts, 429 Rate Limits, and API errors gracefully without breaking the pipeline.
* **Full Test Coverage:** Includes unit tests for data normalization, mathematical calculations (volatility), and async semaphore limits.

## Getting Started

### Installation
1.  **Clone the repo:**
    ```bash
    git clone [https://github.com/dartjomd/crypto_analysis_pandas_project.git](https://github.com/dartjomd/crypto_analysis_pandas_project.git)
    cd crypto_etl_pandas
    ```
2.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

### Running the Pipeline
Simply run the main entry point to start the ETL process:
```bash
python run.py