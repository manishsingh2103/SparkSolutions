import pytest
import pandas as pd
from unittest.mock import MagicMock
from importlib.machinery import SourceFileLoader

# Load the streaming_app module from src/
# This assumes pytest is run from the root of the stock_streaming_project directory
streaming_app_path = "src/streaming_app.py"
try:
    app_module = SourceFileLoader("streaming_app", streaming_app_path).load_module()
    fetch_stock_data_with_history = app_module.fetch_stock_data_with_history
except FileNotFoundError:
    # Fallback for different CWD, e.g. if test is run from within /tests
    streaming_app_path_alt = "../src/streaming_app.py"
    app_module = SourceFileLoader("streaming_app", streaming_app_path_alt).load_module()
    fetch_stock_data_with_history = app_module.fetch_stock_data_with_history


@pytest.fixture
def mock_yfinance_ticker(mocker): # mocker is a pytest-mock fixture
    """Mocks yfinance.Ticker."""
    mock_ticker_instance = MagicMock()
    # Patch 'yfinance.Ticker' within the context of the loaded app_module
    mocker.patch.object(app_module.yf, 'Ticker', return_value=mock_ticker_instance)
    return mock_ticker_instance

def test_fetch_successful(mock_yfinance_ticker):
    """Test successful data fetching when yfinance returns valid historical data."""
    sample_data = {
        'Open': [100.0, 101.0], 'High': [102.0, 103.0], 'Low': [99.0, 100.0],
        'Close': [101.0, 102.0], 'Volume': [1000.0, 1100.0] 
        # yfinance usually returns float for OHLVC, and Volume can be float
    }
    # Create an index that matches yfinance's typical DatetimeIndex
    sample_index = pd.to_datetime(['2023-01-01', '2023-01-02'])
    sample_df = pd.DataFrame(sample_data, index=sample_index)
    sample_df.index.name = 'Date' # yfinance often has 'Date' or 'Datetime' as index name

    mock_yfinance_ticker.history.return_value = sample_df
    
    result_df = fetch_stock_data_with_history("TESTSTOCK")
    
    assert result_df is not None
    assert not result_df.empty
    
    # fetch_stock_data_with_history calls reset_index()
    expected_df = sample_df.reset_index()
    
    # Ensure columns are in the same order for comparison, and types match
    # The 'Date' column in result_df will be 'Datetime' after reset_index from yfinance
    expected_df['Date'] = pd.to_datetime(expected_df['Date'])
    result_df['Date'] = pd.to_datetime(result_df['Date'])

    # Convert all numeric columns to float64 to avoid type mismatches (e.g. int vs float)
    for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
        expected_df[col] = expected_df[col].astype('float64')
        if col in result_df: # result_df might be None or empty
             result_df[col] = result_df[col].astype('float64')

    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=True)

def test_fetch_empty_yfinance_response(mock_yfinance_ticker):
    """Test behavior when yfinance.history() returns an empty DataFrame."""
    mock_yfinance_ticker.history.return_value = pd.DataFrame(columns=['Open', 'High', 'Low', 'Close', 'Volume'])
    
    result_df = fetch_stock_data_with_history("EMPTYSTOCK")
    
    # The function should return None if yfinance returns an empty DataFrame
    assert result_df is None 

def test_fetch_yfinance_exception(mock_yfinance_ticker):
    """Test behavior when yfinance.history() raises an exception."""
    mock_yfinance_ticker.history.side_effect = Exception("Simulated yfinance API error")
    
    result_df = fetch_stock_data_with_history("ERRORSTOCK")
    
    # The function should return None if yfinance raises an exception
    assert result_df is None

def test_fetch_missing_close_column(mock_yfinance_ticker):
    """Test behavior when yfinance returns data missing the 'Close' column."""
    sample_data = {
        'Open': [100, 101], 'High': [102, 103], 'Low': [99, 100],
        'Volume': [1000, 1100] # 'Close' is missing
    }
    sample_df = pd.DataFrame(sample_data, index=pd.to_datetime(['2023-01-01', '2023-01-02']))
    mock_yfinance_ticker.history.return_value = sample_df

    result_df = fetch_stock_data_with_history("MISSINGCLOSESTOCK")
    
    # Function should return None as 'Close' is critical for TA
    assert result_df is None

def test_fetch_partial_ohlcv_columns(mock_yfinance_ticker):
    """Test behavior when yfinance returns some but not all OHLCV columns (but Close is present)."""
    sample_data = {
        'Open': [100, 101], 
        # 'High': [102, 103], # Missing High
        'Low': [99, 100],
        'Close': [101, 102], 
        'Volume': [1000, 1100]
    }
    sample_index = pd.to_datetime(['2023-01-01', '2023-01-02'])
    sample_df = pd.DataFrame(sample_data, index=sample_index)
    sample_df.index.name = 'Date'
    
    mock_yfinance_ticker.history.return_value = sample_df
    
    result_df = fetch_stock_data_with_history("PARTIALCOLSTOCK")
    
    assert result_df is not None
    assert not result_df.empty
    # The function should still return the DataFrame it got, as 'Close' is present.
    # Indicators might fail later, but fetching should pass.
    expected_df = sample_df.reset_index()
    expected_df['Date'] = pd.to_datetime(expected_df['Date'])
    result_df['Date'] = pd.to_datetime(result_df['Date'])

    # Convert relevant columns to float64 for comparison
    for col in ['Open', 'Low', 'Close', 'Volume']: # 'High' is missing
        if col in expected_df:
             expected_df[col] = expected_df[col].astype('float64')
        if col in result_df:
             result_df[col] = result_df[col].astype('float64')
    
    # High column will be missing in both or be NaN depending on how it's handled.
    # The current fetch_stock_data_with_history doesn't add missing OHLCV columns if not present.
    # It only checks if all are present for a log message.
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=True)

# Example of how to run (conceptual for the worker):
# 1. Ensure PYTHONPATH includes the project root or similar for imports to work,
#    OR rely on pytest discovering tests when run from project root.
# 2. Command: pytest tests/test_data_fetching.py
#    (Or just `pytest` from the project root: `stock_streaming_project/`)
