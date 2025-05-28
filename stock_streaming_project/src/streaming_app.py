import logging
import pandas as pd
from pyspark.sql import SparkSession #, functions as F # F not used yet
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import yfinance as yf
import pandas as pd # Ensure pandas is imported for Timestamp
import ta # Import the technical analysis library

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_spark_session():
    """Creates and returns a SparkSession."""
    try:
        spark = SparkSession.builder \
            .appName("StockStreamingApp") \
            .getOrCreate()
        logging.info("SparkSession created successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error creating SparkSession: {e}")
        raise

def fetch_stock_data(stock_symbol: str):
    """
    Fetches live/recent stock data for the given stock symbol using nsepythonserver.
    Returns a dictionary or a Pandas DataFrame with the latest data.
    """
    try:
        logging.info(f"Fetching data for stock symbol: {stock_symbol} using yfinance")
        # Append .NS for NSE listed stocks. yfinance uses ".NS" for NSE.
        ticker_symbol_yf = stock_symbol + ".NS"
        ticker = yf.Ticker(ticker_symbol_yf)

        # Fetch ticker info. This can be a large dictionary.
        # ticker.fast_info is a lighter alternative but might lack some fields.
        # Let's try ticker.info first as requested.
        info = ticker.info
        logging.info(f"Fetched ticker.info for {ticker_symbol_yf}: {info}")

        if not info or info.get('regularMarketPrice') is None and info.get('currentPrice') is None:
            logging.warning(f"ticker.info for {ticker_symbol_yf} is empty or LTP is missing. Info: {info}")
            # Try fetching history for the last available price if info is insufficient
            hist = ticker.history(period="1d")
            if not hist.empty:
                last_close = hist['Close'].iloc[-1]
                last_volume = hist['Volume'].iloc[-1]
                timestamp = pd.Timestamp.now() # Current time for this fallback
                logging.info(f"Falling back to ticker.history for {ticker_symbol_yf}: LTP={last_close}, Volume={last_volume}")
                extracted_data = {
                    "symbol": stock_symbol, # Original symbol
                    "ltp": float(last_close) if pd.notnull(last_close) else None,
                    "volume": int(last_volume) if pd.notnull(last_volume) else None,
                    "timestamp": timestamp
                }
                if extracted_data["ltp"] is None:
                    logging.warning(f"Could not retrieve LTP even from history for {ticker_symbol_yf}.")
                    return None
                return pd.DataFrame([extracted_data])
            else:
                logging.warning(f"No data in ticker.history for {ticker_symbol_yf} either.")
                return None

        # Extract data from ticker.info
        # LTP: 'currentPrice' or 'regularMarketPrice' or 'bid'
        ltp = info.get('currentPrice') or info.get('regularMarketPrice') or info.get('bid')
        if ltp is None: # If primary keys are None, check 'ask' or 'dayHigh'/'dayLow' as last resort
            ltp = info.get('ask') or (info.get('dayHigh') + info.get('dayLow')) / 2

        # Volume: 'volume' or 'regularMarketVolume'
        volume = info.get('volume') or info.get('regularMarketVolume')
        
        # Timestamp: yfinance `info` doesn't provide a direct last trade timestamp.
        # We'll use current timestamp. For more accurate trade time, ticker.history() is needed for specific intervals.
        timestamp = pd.Timestamp.now() # Current time of fetching

        if ltp is None:
            logging.warning(f"Could not extract LTP for {ticker_symbol_yf} from ticker.info. Info: {info}")
            return None # Or return empty DataFrame as per requirements

        extracted_data = {
            "symbol": stock_symbol, # Original symbol, not ticker_symbol_yf
            "ltp": float(ltp),
            "volume": int(volume) if volume is not None else None,
            "timestamp": timestamp
        }
        logging.info(f"Data extracted for {stock_symbol}: {extracted_data}")
        return pd.DataFrame([extracted_data])

    except Exception as e:
        logging.error(f"Error fetching/processing stock data for {stock_symbol} using yfinance: {e}")
        # Return empty DataFrame with expected columns or None
        # For now, returning None as per previous behavior for critical errors.
        # return pd.DataFrame(columns=['symbol', 'ltp', 'volume', 'timestamp'])
        return None

def fetch_stock_data_with_history(stock_symbol: str, history_period: str = "90d", interval: str = "1d"):
    """
    Fetches historical stock data for the given stock symbol using yfinance.
    Returns a Pandas DataFrame with Open, High, Low, Close, Volume.
    """
    try:
        ticker_symbol_yf = stock_symbol + ".NS" # Append .NS for NSE listed stocks
        ticker = yf.Ticker(ticker_symbol_yf)
        history_df = ticker.history(period=history_period, interval=interval)

        if history_df.empty:
            logging.warning(f"No historical data fetched for {ticker_symbol_yf} for period {history_period}.")
            return None
        
        # Ensure required columns are present, especially 'Close'
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        if not all(col in history_df.columns for col in required_cols):
            logging.warning(f"Historical data for {ticker_symbol_yf} is missing one of {required_cols}. Columns found: {history_df.columns.tolist()}")
            # Attempt to use it anyway if 'Close' is present, otherwise fail
            if 'Close' not in history_df.columns:
                logging.error(f"'Close' column missing in historical data for {ticker_symbol_yf}, cannot calculate indicators.")
                return None
        
        logging.info(f"Successfully fetched historical data for {ticker_symbol_yf} with {len(history_df)} rows.")
        return history_df.reset_index() # Reset index to make 'Date' (or 'Datetime') a column if needed, though not strictly for ta
    except Exception as e:
        logging.error(f"Error fetching historical data for {stock_symbol} using yfinance: {e}")
        return None

if __name__ == "__main__":
    spark = create_spark_session()

    sample_stock_symbol = "SBIN"  # State Bank of India
    # Define a list of stock symbols to track
    stock_symbols = ["SBIN", "INFY", "RELIANCE"] # Add more symbols as needed

    # Define a Spark Schema for the stock data
    # stock_data_schema is not directly used for indicators_schema, but good to keep if current price stream is separate
    # stock_data_schema = StructType([
    #     StructField("symbol", StringType(), True),
    #     StructField("ltp", DoubleType(), True),
    #     StructField("volume", DoubleType(), True), 
    #     StructField("timestamp", TimestampType(), True)
    # ])

    # Define a new Spark Schema for the data with technical indicators and signals
    signals_output_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("Date", TimestampType(), True), # From yfinance history index
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", DoubleType(), True), # yfinance volume can be large
        StructField("sma_20", DoubleType(), True),
        StructField("sma_50", DoubleType(), True), # Added for SMA crossover
        StructField("ema_20", DoubleType(), True),
        StructField("rsi_14", DoubleType(), True),
        StructField("macd", DoubleType(), True),
        StructField("macd_signal", DoubleType(), True),
        StructField("macd_diff", DoubleType(), True),
        StructField("bb_high_band", DoubleType(), True),
        StructField("bb_low_band", DoubleType(), True),
        StructField("bb_ma", DoubleType(), True),
        StructField("sma_signal", StringType(), True),
        StructField("rsi_signal", StringType(), True),
        StructField("macd_signal_col", StringType(), True), # Renamed to avoid conflict with macd_signal data field
        StructField("bb_signal", StringType(), True)
    ])
    
    # Function to process each micro-batch
    def process_micro_batch(batch_df, epoch_id):
        # batch_df from rate source is small, just used to trigger the fetch
        logging.info(f"--- Processing batch {epoch_id} for indicators and signals ---")
        
        all_stocks_with_indicators_pdf_list = []
        active_signals_config = {"SMA": True, "RSI": True, "MACD": True, "BB": True}

        for symbol in stock_symbols:
            hist_pdf = fetch_stock_data_with_history(symbol) # Fetch historical data
            
            if hist_pdf is not None and not hist_pdf.empty and 'Close' in hist_pdf.columns:
                try:
                    # Calculate Indicators using `ta`
                    hist_pdf['sma_20'] = ta.trend.SMAIndicator(hist_pdf['Close'], window=20, fillna=True).sma_indicator()
                    hist_pdf['sma_50'] = ta.trend.SMAIndicator(hist_pdf['Close'], window=50, fillna=True).sma_indicator() # Added SMA 50
                    hist_pdf['ema_20'] = ta.trend.EMAIndicator(hist_pdf['Close'], window=20, fillna=True).ema_indicator()
                    hist_pdf['rsi_14'] = ta.momentum.RSIIndicator(hist_pdf['Close'], window=14, fillna=True).rsi()
                    
                    macd_obj = ta.trend.MACD(hist_pdf['Close'], fillna=True)
                    hist_pdf['macd'] = macd_obj.macd()
                    hist_pdf['macd_signal_data'] = macd_obj.macd_signal() # Renamed data field
                    hist_pdf['macd_diff'] = macd_obj.macd_diff()
                    
                    bollinger_obj = ta.volatility.BollingerBands(hist_pdf['Close'], window=20, window_dev=2, fillna=True)
                    hist_pdf['bb_high_band'] = bollinger_obj.bollinger_hband()
                    hist_pdf['bb_low_band'] = bollinger_obj.bollinger_lband()
                    hist_pdf['bb_ma'] = bollinger_obj.bollinger_mavg()

                    # Keep only the last row as it represents the latest indicator values
                    latest_indicators_pdf = hist_pdf.iloc[[-1]].copy() # Keep as DataFrame
                    latest_indicators_pdf['symbol'] = symbol # Add symbol column
                    
                    # Ensure 'Date' column is present (it's the index from yfinance history)
                    if 'Date' not in latest_indicators_pdf.columns and isinstance(latest_indicators_pdf.index, pd.DatetimeIndex):
                         latest_indicators_pdf['Date'] = latest_indicators_pdf.index.tz_localize(None) # Remove timezone for Spark
                    elif 'Date' in latest_indicators_pdf.columns:
                         latest_indicators_pdf['Date'] = pd.to_datetime(latest_indicators_pdf['Date']).dt.tz_localize(None)


                    all_stocks_with_indicators_pdf_list.append(latest_indicators_pdf)
                    logging.info(f"Calculated indicators for {symbol} for date {latest_indicators_pdf['Date'].iloc[0] if 'Date' in latest_indicators_pdf.columns and not latest_indicators_pdf['Date'].empty else 'N/A'}")

                except Exception as e:
                    logging.error(f"Error calculating indicators for {symbol}: {e}")
                    logging.debug(f"Data for {symbol} that caused error:\n{hist_pdf.tail()}")
            else:
                logging.warning(f"Could not fetch or 'Close' column missing in historical data for {symbol}.")

        if all_stocks_with_indicators_pdf_list:
            pdf_for_signals = pd.concat(all_stocks_with_indicators_pdf_list, ignore_index=True)
            
            # Initialize signal columns
            pdf_for_signals['sma_signal'] = "HOLD"
            pdf_for_signals['rsi_signal'] = "HOLD"
            pdf_for_signals['macd_signal_col'] = "HOLD" # Renamed to avoid conflict
            pdf_for_signals['bb_signal'] = "HOLD"

            # SMA Crossover Signal
            if active_signals_config.get("SMA", False) and 'sma_20' in pdf_for_signals.columns and 'sma_50' in pdf_for_signals.columns:
                pdf_for_signals.loc[pdf_for_signals['sma_20'] > pdf_for_signals['sma_50'], 'sma_signal'] = "BUY"
                pdf_for_signals.loc[pdf_for_signals['sma_20'] < pdf_for_signals['sma_50'], 'sma_signal'] = "SELL"

            # RSI Signal
            if active_signals_config.get("RSI", False) and 'rsi_14' in pdf_for_signals.columns:
                pdf_for_signals.loc[pdf_for_signals['rsi_14'] < 30, 'rsi_signal'] = "BUY"
                pdf_for_signals.loc[pdf_for_signals['rsi_14'] > 70, 'rsi_signal'] = "SELL"

            # MACD Signal
            if active_signals_config.get("MACD", False) and 'macd' in pdf_for_signals.columns and 'macd_signal_data' in pdf_for_signals.columns:
                pdf_for_signals.loc[pdf_for_signals['macd'] > pdf_for_signals['macd_signal_data'], 'macd_signal_col'] = "BUY"
                pdf_for_signals.loc[pdf_for_signals['macd'] < pdf_for_signals['macd_signal_data'], 'macd_signal_col'] = "SELL"
            
            # Bollinger Bands Signal
            if active_signals_config.get("BB", False) and 'Close' in pdf_for_signals.columns and 'bb_low_band' in pdf_for_signals.columns and 'bb_high_band' in pdf_for_signals.columns:
                pdf_for_signals.loc[pdf_for_signals['Close'] < pdf_for_signals['bb_low_band'], 'bb_signal'] = "BUY"
                pdf_for_signals.loc[pdf_for_signals['Close'] > pdf_for_signals['bb_high_band'], 'bb_signal'] = "SELL"

            # Ensure all schema columns are present, fill with None if not
            # This includes indicator columns from indicators_schema and new signal columns
            for col_name in signals_output_schema.names:
                if col_name not in pdf_for_signals.columns:
                    pdf_for_signals[col_name] = None 
            
            # Rename macd_signal_data to macd_signal for schema matching
            if 'macd_signal_data' in pdf_for_signals.columns:
                pdf_for_signals.rename(columns={'macd_signal_data': 'macd_signal'}, inplace=True)


            # Explicitly cast types for Spark compatibility if needed
            if 'Volume' in pdf_for_signals.columns:
                pdf_for_signals['Volume'] = pdf_for_signals['Volume'].astype(float)
            if 'Date' not in pdf_for_signals.columns or pdf_for_signals['Date'].isnull().all():
                 logging.warning("'Date' column is missing or all null before Spark DF creation. Using current time.")
                 pdf_for_signals['Date'] = pd.Timestamp.now(tz='UTC').tz_localize(None) # Ensure timezone naive
            else:
                 pdf_for_signals['Date'] = pd.to_datetime(pdf_for_signals['Date']).dt.tz_localize(None)


            try:
                # Create a Spark DataFrame for the current batch with indicators and signals
                signals_spark_df = spark.createDataFrame(pdf_for_signals, schema=signals_output_schema)
                logging.info(f"Successfully created Spark DataFrame with signals for batch {epoch_id}. Showing data:")
                signals_spark_df.show(truncate=False)
            except Exception as e:
                logging.error(f"Error creating Spark DataFrame with signals in batch {epoch_id}: {e}")
                logging.error(f"Pandas DataFrame that caused error (signals_output_schema):\n{pdf_for_signals.head().to_string()}")
                logging.error(f"Pandas DataFrame dtypes (signals_output_schema):\n{pdf_for_signals.dtypes}")
        else:
            logging.info(f"No data or indicators processed in batch {epoch_id} to generate signals.")

    # Initial stream from rate source
    # Trigger e.g. 1 row every 1 second.
    # The value for rowsPerSecond must be an integer.
    rate_stream_df = spark.readStream.format("rate").option("rowsPerSecond", 1).load() 

    # Process each batch using foreachBatch
    query = rate_stream_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .outputMode("update") \
        .start()
    
    logging.info("Streaming query started. Waiting for termination (e.g. Ctrl+C)...")
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logging.info("Streaming query interrupted by user.")
    finally:
        logging.info("Stopping SparkSession...")
        if spark:
            spark.stop()
            logging.info("SparkSession stopped.")
