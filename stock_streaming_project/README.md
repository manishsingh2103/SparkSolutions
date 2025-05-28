# Project Title
Real-Time Stock Analysis and Signal Generation

## Description
This project is a Python-based application that utilizes PySpark to stream stock data, calculate various technical indicators, and generate trading signals. It fetches live stock data using the `yfinance` library and performs technical analysis using the `ta` library. The primary goal is to demonstrate a scalable streaming pipeline for financial data processing and signal generation.

## Features
- Real-time (simulated via frequent batching) stock data fetching for multiple symbols.
- Calculation of common technical indicators: Simple Moving Average (SMA), Exponential Moving Average (EMA), Relative Strength Index (RSI), Moving Average Convergence Divergence (MACD), and Bollinger Bands.
- Generation of BUY/SELL/HOLD trading signals based on configurable indicator logic.
- Utilizes Spark Structured Streaming for robust and scalable data processing.
- Dockerized environment for simplified setup, deployment, and consistent execution.
- Includes unit tests for core components to ensure reliability and correctness.

## Technologies Used
- Python
- PySpark
- yfinance
- pandas
- ta (Technical Analysis Library)
- Docker
- pytest (for testing)

## Setup and Installation

### Prerequisites
- Git
- Docker Engine (and Docker Compose, usually included with Docker Desktop)
- Python (e.g., 3.8+) (for alternative local setup or code exploration outside Docker)

### Steps
1.  **Clone the repository:**
    ```bash
    git clone <repository_url> # Replace <repository_url> with your repository's URL
    cd stock_streaming_project
    ```

2.  **Build and Run with Docker Compose (Recommended):**
    Docker provides a consistent and isolated environment for running the application.
    ```bash
    docker-compose build
    docker-compose up
    ```
    To run in detached mode (in the background), you can use:
    ```bash
    docker-compose up -d
    ```

3.  **Alternative: Local Python Environment (Optional):**
    If you prefer to set up a local Python environment:
    - Create and activate a virtual environment:
      ```bash
      python -m venv venv
      source venv/bin/activate  # On Windows use: venv\Scripts\activate
      ```
    - Install dependencies (ensure you have a `requirements.txt` file):
      ```bash
      pip install -r requirements.txt
      ```
    *Note: Running PySpark applications locally without Docker might require additional setup for Spark itself (e.g., downloading Spark, setting `SPARK_HOME`, and `PATH` environment variables). For simplicity and consistency, Docker is the recommended approach.*

## Usage

This section assumes you are using Docker Compose as recommended in the "Setup and Installation" section.

1.  **Starting the Application:**
    Navigate to the project's root directory (where `docker-compose.yml` is located) and run:
    ```bash
    docker-compose up
    ```
    This will start the PySpark application, and you will see logs in your terminal.

2.  **Viewing Logs (if running in detached mode):**
    If you started the application in detached mode using `docker-compose up -d`, you can view the logs from the `spark-stock-app` service (as defined in `docker-compose.yml`) using:
    ```bash
    docker-compose logs -f spark-stock-app
    ```
    The `-f` flag follows the log output, similar to `tail -f`.

3.  **What the Application Does:**
    Once running, the application will:
    - Begin fetching near real-time stock data for a predefined list of stock symbols (e.g., SBIN, INFY, RELIANCE - update these if your `config.py` uses different symbols).
    - Continuously calculate various technical indicators for each stock.
    - Generate trading signals (BUY, SELL, HOLD) based on the calculated indicators.
    - Print the latest processed data, including indicators and signals for each stock, to the console output (stdout) of the `spark-stock-app` container. This output will appear periodically as new data is processed by the Spark Structured Streaming query.

4.  **Stopping the Application:**
    - If you ran `docker-compose up` in the foreground, press `Ctrl+C` in the terminal where it's running.
    - If you ran `docker-compose up -d` (detached mode), stop the services using:
      ```bash
      docker-compose down
      ```
      This command stops and removes the containers, networks, and volumes created by `docker-compose up`.

## Project Structure

-   `src/`: Contains the main application logic.
    -   `streaming_app.py`: The core PySpark streaming application that fetches data, calculates indicators, and generates signals.
-   `tests/`: Contains unit tests for the application.
    -   `test_data_fetching.py`: Tests for the data fetching module.
    -   `test_indicator_calculation.py`: Tests for the technical indicator calculation logic.
    -   `test_signal_generation.py`: Tests for the signal generation logic.
-   `Dockerfile`: Defines the Docker image for the application, including environment setup and dependencies.
-   `docker-compose.yml`: Docker Compose file to build and run the application service (`spark-stock-app`).
-   `requirements.txt`: Lists the Python dependencies for the project.
-   `README.md`: This file - provides information about the project.
-   `cookies.txt`: This file is present; its specific use in the current application version is not detailed in the core streaming logic. It might be related to other utilities or a previous version.

## How it Works

The application follows these steps to process and analyze stock data:

1.  **Spark Session Initialization:**
    -   The application begins by creating a `SparkSession`, which is the entry point to any Spark functionality.

2.  **Streaming Trigger (Timer):**
    -   A rate stream (`spark.readStream.format("rate").option("rowsPerSecond", 1).load()`) is configured. This stream doesn't consume external data but acts as a periodic trigger, initiating a micro-batch processing cycle at a defined interval (e.g., every second).

3.  **Data Fetching (within each micro-batch):**
    -   For each stock symbol specified in a predefined list (e.g., "SBIN", "INFY", "RELIANCE"):
        -   The `fetch_stock_data_with_history()` function is called. This function uses the `yfinance` library to download historical OHLCV (Open, High, Low, Close, Volume) data for a significant period (e.g., the last 90 days). This historical data is crucial for calculating meaningful technical indicators.
        -   (Note: While a `fetch_stock_data()` function might exist for fetching only the current price, the historical data obtained via `fetch_stock_data_with_history()` is what's primarily used for the technical indicators listed below.)

4.  **Technical Indicator Calculation:**
    -   The historical data for each stock, now typically a Pandas DataFrame, is processed using the `ta` library to compute various technical indicators. These include:
        -   Simple Moving Averages (SMA), for example, 20-day and 50-day.
        -   Exponential Moving Average (EMA), for example, 20-day.
        -   Relative Strength Index (RSI), for example, 14-day.
        -   Moving Average Convergence Divergence (MACD).
        -   Bollinger Bands.
    -   The calculations are performed on the historical data, but generally, only the most recent set of indicator values (corresponding to the latest data point in the fetched history) is used for generating trading signals in that micro-batch.

5.  **Signal Generation:**
    -   Using the latest calculated technical indicator values for each stock, trading signals (BUY, SELL, or HOLD) are generated.
    -   The specific logic (e.g., RSI thresholds, moving average crossovers) used to determine these signals is defined within the application (and detailed further in the "Signal Generation Logic" section).

6.  **Output:**
    -   The application aggregates the results into a Spark DataFrame. This DataFrame includes columns for the stock symbol, its latest calculated technical indicators, and the generated trading signal.
    -   This DataFrame is then displayed in the console using `signals_spark_df.show(truncate=False)`, providing a snapshot of the analysis for each micro-batch.

7.  **Streaming Query Management:**
    -   The Spark Structured Streaming query is started, and `query.awaitTermination()` is called. This keeps the application alive and continuously processing data in micro-batches until it is manually stopped (e.g., by pressing `Ctrl+C` in the terminal).

## Technical Indicators Calculated

The application calculates the following technical indicators using the `ta` library, based on the historical closing prices fetched by `yfinance`. The parameters mentioned are typical and can be configured within `src/streaming_app.py`.

-   **SMA (Simple Moving Average):** Calculated for 20-day and 50-day periods.
    -   SMA_20: `ta.trend.SMAIndicator(close=df['Close'], window=20)`
    -   SMA_50: `ta.trend.SMAIndicator(close=df['Close'], window=50)`
-   **EMA (Exponential Moving Average):** Calculated for a 20-day period.
    -   EMA_20: `ta.trend.EMAIndicator(close=df['Close'], window=20)`
-   **RSI (Relative Strength Index):** Calculated for a 14-day period.
    -   RSI: `ta.momentum.RSIIndicator(close=df['Close'], window=14)`
-   **MACD (Moving Average Convergence Divergence):** Uses default parameters (fast=12 periods, slow=26 periods, signal=9 periods).
    -   MACD Line: `ta.trend.MACD(close=df['Close']).macd()`
    -   Signal Line: `ta.trend.MACD(close=df['Close']).macd_signal()`
    -   Histogram (Difference): `ta.trend.MACD(close=df['Close']).macd_diff()`
-   **Bollinger Bands:** Calculated with a 20-day SMA and 2 standard deviations.
    -   Upper Band: `ta.volatility.BollingerBands(close=df['Close'], window=20, window_dev=2).bollinger_hband()`
    -   Lower Band: `ta.volatility.BollingerBands(close=df['Close'], window=20, window_dev=2).bollinger_lband()`
    -   Moving Average (Center Line): `ta.volatility.BollingerBands(close=df['Close'], window=20, window_dev=2).bollinger_mavg()`

## Signal Generation Logic

The trading signals are generated based on the latest values of the calculated technical indicators. The logic for each signal is as follows (based on common interpretations and likely implementation in `src/streaming_app.py`):

-   **SMA Crossover Signal:**
    -   **BUY:** When the short-term SMA (SMA_20) crosses above the long-term SMA (SMA_50). (Specifically, `SMA_20 > SMA_50` in the latest period, and `SMA_20_previous <= SMA_50_previous`).
    -   **SELL:** When the short-term SMA (SMA_20) crosses below the long-term SMA (SMA_50). (Specifically, `SMA_20 < SMA_50` in the latest period, and `SMA_20_previous >= SMA_50_previous`).
    -   **HOLD:** Otherwise.

-   **RSI Signal:**
    -   **BUY:** When RSI is below 30 (indicating an oversold condition).
    -   **SELL:** When RSI is above 70 (indicating an overbought condition).
    -   **HOLD:** When RSI is between 30 and 70.

-   **MACD Signal:**
    -   **BUY:** When the MACD line crosses above the MACD signal line. (Specifically, `MACD_line > MACD_signal_line` in the latest period, and `MACD_line_previous <= MACD_signal_line_previous`).
    -   **SELL:** When the MACD line crosses below the MACD signal line. (Specifically, `MACD_line < MACD_signal_line` in the latest period, and `MACD_line_previous >= MACD_signal_line_previous`).
    -   **HOLD:** Otherwise.

-   **Bollinger Bands Signal:**
    -   **BUY:** When the closing price crosses below the lower Bollinger Band (potentially indicating an oversold condition or bounce opportunity).
    -   **SELL:** When the closing price crosses above the upper Bollinger Band (potentially indicating an overbought condition or reversal opportunity).
    -   **HOLD:** When the price is within the Bollinger Bands.

-   **Overall Signal:**
    -   The application, as described, appears to generate and present these signals individually for each indicator rather than combining them into a single, unified BUY/SELL/HOLD signal. The output typically shows a signal for each strategy (SMA Crossover, RSI, MACD, Bollinger Bands) for each stock.

## Running Tests

The project includes unit tests for core functionalities, located in the `tests/` directory. These tests are written using the `pytest` framework.

1.  **Running Tests with Docker (Recommended):**
    Running tests inside the Docker container ensures that the testing environment is consistent with the application's execution environment.
    -   If your Docker services are already running (e.g., via `docker-compose up -d`), you can execute the tests in the `spark-stock-app` container using:
        ```bash
        docker-compose exec spark-stock-app pytest tests/
        ```
    -   This command specifically targets the `tests/` directory. Running `pytest` without arguments might also work if `pytest` is configured to discover tests automatically and the working directory is appropriate within the container.
    -   If the service name in your `docker-compose.yml` is different from `spark-stock-app`, replace it accordingly in the command.

2.  **Running Tests Locally (Alternative):**
    If you have set up a local Python environment as described in the "Setup and Installation" section:
    -   Ensure that `pytest` and all other project dependencies are installed from `requirements.txt`:
        ```bash
        pip install -r requirements.txt
        ```
    -   Navigate to the root directory of the `stock_streaming_project`.
    -   Run the tests using `pytest`. You can either specify the tests directory or let `pytest` discover them:
        ```bash
        pytest tests/
        ```
        or simply:
        ```bash
        pytest
        ```
    -   `pytest` will automatically discover and run the tests in the `tests/` directory.

## Future Enhancements

Here are some potential improvements and future development ideas for this project:

-   **Persistent Storage:** Implement saving the generated signals and indicator data to a database (e.g., PostgreSQL, InfluxDB) or a distributed file system (e.g., HDFS, S3 for Parquet/CSV files) instead of just printing to the console. This would allow for historical analysis and more robust data management.
-   **Configuration File:** Allow users to configure stock symbols, indicator parameters (like SMA/EMA windows, RSI periods), and signal generation thresholds through an external configuration file (e.g., YAML, JSON, or `.env` file). This would make the application more flexible and easier to customize without code changes.
-   **Expanded Indicator Library:** Integrate a wider range of technical indicators from the `ta` library or other financial analysis libraries.
-   **Advanced Signal Logic:** Develop more sophisticated signal generation strategies. This could involve:
    -   Combining signals from multiple indicators (e.g., requiring both RSI and MACD to indicate a BUY).
    -   Implementing weighted scoring systems for signals.
    -   Using machine learning models to predict price movements or generate signals.
-   **Web Interface/Dashboard:** Create a web interface or dashboard (e.g., using Flask, Django, Dash, or Streamlit) to visualize the live stock data, technical indicators, generated signals, and potentially historical performance.
-   **Alerting System:** Implement an alerting mechanism (e.g., via email, SMS, or services like PagerDuty/Slack) for when specific BUY/SELL signals are generated for particular stocks, or when certain market conditions are met.
-   **Backtesting Framework:** Integrate or develop a robust backtesting framework to evaluate the historical performance of the trading strategies implemented. This would allow for strategy optimization and risk assessment.
-   **Real-time Data Source Integration:** Replace the current simulated streaming (fetching historical data frequently) with a true real-time data source. This could involve connecting to:
    -   A Kafka stream fed by a live market data provider.
    -   WebSocket APIs from stock exchanges or brokers.
-   **Improved Error Handling and Resilience:** Enhance error handling for data fetching, API interactions, and Spark processing to make the application more robust and fault-tolerant.
-   **Scalability and Performance Optimization:** Further optimize Spark configurations and data processing logic for handling a larger number of stock symbols or higher data velocities.
-   **More Sophisticated State Management:** For more complex strategies that rely on state across micro-batches (beyond simple windowing), explore advanced Spark state management techniques.
