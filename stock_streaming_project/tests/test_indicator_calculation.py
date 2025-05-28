import pytest
import pandas as pd
import ta # Main import
from ta.utils import dropna # For cleaning NaNs if needed for specific assertions

# Sample data (expand this to be more comprehensive)
@pytest.fixture
def sample_ohlcv_data():
    # Using a slightly more realistic dataset (e.g., 60 periods)
    data = {
        'Close': [
            100.0, 101.0, 102.5, 101.5, 103.0, 104.5, 105.0, 106.5, 105.5, 107.0, #10
            108.5, 109.0, 110.5, 109.5, 111.0, 112.5, 113.0, 114.5, 113.5, 115.0, #20
            116.5, 117.0, 118.5, 117.5, 119.0, 120.5, 121.0, 122.5, 121.5, 123.0, #30
            124.5, 125.0, 126.5, 125.5, 127.0, 128.5, 129.0, 130.5, 129.5, 131.0, #40
            130.0, 128.5, 127.0, 126.5, 125.0, 123.5, 122.0, 120.5, 119.0, 117.5, #50
            116.0, 114.5, 113.0, 111.5, 110.0, 108.5, 107.0, 105.5, 104.0, 102.5  #60
        ]
    }
    df = pd.DataFrame(data)
    # Add Open, High, Low, Volume if needed for specific ta indicators,
    # but most common ones use 'Close'.
    df['Open'] = df['Close'] - 0.5 
    df['High'] = df['Close'] + 0.5
    df['Low'] = df['Close'] - 1.0
    df['Volume'] = 1000.0 # Keep as float to match yfinance typical output
    return df

def test_sma_calculation(sample_ohlcv_data):
    df = sample_ohlcv_data.copy()
    sma20 = ta.trend.SMAIndicator(df['Close'], window=20, fillna=True).sma_indicator()
    sma50 = ta.trend.SMAIndicator(df['Close'], window=50, fillna=True).sma_indicator()
    
    assert not sma20.empty
    assert not sma50.empty
    assert sma20.isna().sum() == 19 # First 19 values are NaN for window 20
    assert sma50.isna().sum() == 49 # First 49 values are NaN for window 50
    
    # Check the last calculated value against pandas rolling mean
    assert sma20.iloc[-1] == pytest.approx(df['Close'].iloc[41:61].mean()) # SMA20 for last 20 points (index 40 to 59)
    assert sma50.iloc[-1] == pytest.approx(df['Close'].iloc[10:61].mean()) # SMA50 for last 50 points (index 10 to 59)

def test_rsi_calculation(sample_ohlcv_data):
    df = sample_ohlcv_data.copy()
    rsi14 = ta.momentum.RSIIndicator(df['Close'], window=14, fillna=True).rsi()
    assert not rsi14.empty
    # RSI typically has `window` NaNs at the beginning, but ta library might fill differently or have fewer.
    # For window=14, the first RSI value is typically at index 14.
    # `fillna=True` might affect this, but usually, it means internal NaNs are filled.
    # The actual number of initial NaNs before the first valid RSI can be complex.
    # Let's check if the first valid value is not NaN after the initial period.
    assert not rsi14.iloc[14:].isna().any() # After initial period, no NaNs if fillna=True worked as expected for subsequent calculations
    assert rsi14.dropna().between(0, 100).all() # RSI values must be between 0 and 100

def test_ema_calculation(sample_ohlcv_data):
    df = sample_ohlcv_data.copy()
    ema20 = ta.trend.EMAIndicator(df['Close'], window=20, fillna=True).ema_indicator()
    assert not ema20.empty
    # EMA's `fillna=True` typically fills leading NaNs by using SMA or initial values.
    # The number of NaNs can be less than the window. `ta` library often has 0 NaNs with fillna=True if possible.
    # If fillna=True, it might not have any NaNs if it can compute/forward-fill.
    # With fillna=True, the first value is often the value itself or a very short SMA.
    # Let's check if there are no NaNs if fillna=True is effective.
    assert not ema20.isna().any() # Expect no NaNs if fillna=True is effective.
    # A basic check: EMA should generally follow the trend of the close prices.
    # For an upward trend, EMA should be generally increasing.
    # For the sample data (up then down), this is harder to assert simply.
    # Check if the last EMA value is a number
    assert pd.notna(ema20.iloc[-1])

def test_macd_calculation(sample_ohlcv_data):
    df = sample_ohlcv_data.copy()
    macd_indicator = ta.trend.MACD(df['Close'], window_slow=26, window_fast=12, window_sign=9, fillna=True)
    
    macd_line = macd_indicator.macd()
    signal_line = macd_indicator.macd_signal()
    histogram = macd_indicator.macd_diff()
    
    assert not macd_line.empty
    assert not signal_line.empty
    assert not histogram.empty
    
    # fillna=True should handle most NaNs, but some might remain at the very beginning
    # typically window_slow - 1 + window_sign - 1 for signal line.
    # For MACD line itself, it's window_slow - 1.
    # `ta` with fillna=True aims to reduce NaNs, often filling initial values.
    assert not macd_line.isna().any()
    assert not signal_line.isna().any()
    assert not histogram.isna().any()

    # Check a known relationship: histogram = macd_line - signal_line
    pd.testing.assert_series_equal(histogram, macd_line - signal_line, check_dtype=False, atol=1e-9)

def test_bollinger_bands_calculation(sample_ohlcv_data):
    df = sample_ohlcv_data.copy()
    window = 20
    bb_indicator = ta.volatility.BollingerBands(df['Close'], window=window, window_dev=2, fillna=True)
    
    hband = bb_indicator.bollinger_hband()
    lband = bb_indicator.bollinger_lband()
    mavg = bb_indicator.bollinger_mavg() # Middle band (SMA20)
    
    assert not hband.empty
    assert not lband.empty
    assert not mavg.empty
    
    # With fillna=True, expect no NaNs if calculation is possible throughout
    assert not hband.isna().any()
    assert not lband.isna().any()
    assert not mavg.isna().any()
    
    # Check basic properties (hband >= mavg >= lband)
    # Need to drop initial NaNs if fillna=False was used. With fillna=True, this should hold for all.
    assert (hband >= mavg).all()
    assert (mavg >= lband).all()
    
    # Middle band should be SMA(window)
    expected_mavg = ta.trend.SMAIndicator(df['Close'], window=window, fillna=True).sma_indicator()
    pd.testing.assert_series_equal(mavg, expected_mavg, check_dtype=False, atol=1e-9)
