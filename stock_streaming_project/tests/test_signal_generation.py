import pytest
import pandas as pd

@pytest.fixture
def sample_indicator_data():
    # DataFrame with pre-calculated indicators and 'Close' prices
    # Scenarios:
    # Idx | Close | sma_20 | sma_50 | rsi_14 | macd | macd_signal_data | bb_low | bb_high | Expected SMA | Expected RSI | Expected MACD | Expected BB
    # --- | ----- | ------ | ------ | ------ | ---- | ---------------- | ------ | ------- | ------------ | ------------ | ------------- | -----------
    # 0   | 100   | 100    | 100    | 50     | 0    | 0                | 98     | 102     | HOLD         | HOLD         | HOLD          | HOLD
    # 1   | 105   | 102    | 100    | 80     | 1    | 0.5              | 100    | 110     | BUY          | SELL         | BUY           | HOLD
    # 2   | 95    | 98     | 100    | 20     | -1   | -0.5             | 90     | 100     | SELL         | BUY          | SELL          | HOLD
    # 3   | 110   | 105    | 102    | 50     | 0.5  | 0                | 100    | 120     | BUY          | HOLD         | BUY           | HOLD (explicit test for SMA BUY)
    # 4   | 90    | 95     | 98     | 50     | -0.5 | 0                | 85     | 95      | SELL         | HOLD         | SELL          | HOLD (explicit test for SMA SELL)
    # 5   | 100   | 99     | 101    | 25     | 0.1  | 0                | 99     | 101     | SELL         | BUY          | BUY           | HOLD (explicit test for RSI BUY)
    # 6   | 100   | 101    | 99     | 75     | -0.1 | 0                | 99     | 101     | BUY          | SELL         | SELL          | HOLD (explicit test for RSI SELL)
    # 7   | 100   | 100    | 100    | 50     | 0    | -0.1             | 99     | 101     | HOLD         | HOLD         | BUY           | HOLD (explicit test for MACD BUY)
    # 8   | 100   | 100    | 100    | 50     | 0    | 0.1              | 99     | 101     | HOLD         | HOLD         | SELL          | HOLD (explicit test for MACD SELL)
    # 9   | 90    | 100    | 100    | 50     | 0    | 0                | 95     | 105     | HOLD         | HOLD         | HOLD          | BUY (Close < bb_low_band)
    # 10  | 110   | 100    | 100    | 50     | 0    | 0                | 95     | 105     | HOLD         | HOLD         | HOLD          | SELL (Close > bb_high_band)
    data = {
        'Close':            [100, 105, 95,  110, 90,  100, 100, 100, 100, 90,  110 ],
        'sma_20':           [100, 102, 98,  105, 95,  99,  101, 100, 100, 100, 100 ], 
        'sma_50':           [100, 100, 100, 102, 98,  101, 99,  100, 100, 100, 100 ], 
        'rsi_14':           [50,  80,  20,  50,  50,  25,  75,  50,  50,  50,  50  ],
        'macd':             [0,   1,   -1,  0.5, -0.5, 0.1, -0.1, 0,   0,   0,   0   ],
        'macd_signal_data': [0,   0.5, -0.5, 0,   0,    0,   0,   -0.1, 0.1, 0,   0   ], # Corrected idx 7 & 8 for clear MACD signals
        'bb_low_band':      [98,  100, 90,  100, 85,  99,  99,  99,  99,  95,  95  ], 
        'bb_high_band':     [102, 110, 100, 120, 95,  101, 101, 101, 101, 105, 105 ]  
    }
    df = pd.DataFrame(data)
    return df

def test_sma_signal_generation(sample_indicator_data):
    df = sample_indicator_data.copy()
    df['sma_signal'] = "HOLD" # Initialize
    
    # Apply SMA signal logic (as in streaming_app.py)
    df.loc[df['sma_20'] > df['sma_50'], 'sma_signal'] = "BUY"
    df.loc[df['sma_20'] < df['sma_50'], 'sma_signal'] = "SELL"
    
    # Expected based on data comments
    assert df.loc[0, 'sma_signal'] == "HOLD"
    assert df.loc[1, 'sma_signal'] == "BUY" 
    assert df.loc[2, 'sma_signal'] == "SELL"
    assert df.loc[3, 'sma_signal'] == "BUY"
    assert df.loc[4, 'sma_signal'] == "SELL"
    assert df.loc[5, 'sma_signal'] == "SELL" # sma_20 (99) < sma_50 (101)
    assert df.loc[6, 'sma_signal'] == "BUY"  # sma_20 (101) > sma_50 (99)

def test_rsi_signal_generation(sample_indicator_data):
    df = sample_indicator_data.copy()
    df['rsi_signal'] = "HOLD" # Initialize

    # Apply RSI signal logic
    df.loc[df['rsi_14'] < 30, 'rsi_signal'] = "BUY"
    df.loc[df['rsi_14'] > 70, 'rsi_signal'] = "SELL"

    assert df.loc[0, 'rsi_signal'] == "HOLD"  # rsi_14 = 50
    assert df.loc[1, 'rsi_signal'] == "SELL"  # rsi_14 = 80 
    assert df.loc[2, 'rsi_signal'] == "BUY"   # rsi_14 = 20
    assert df.loc[5, 'rsi_signal'] == "BUY"   # rsi_14 = 25
    assert df.loc[6, 'rsi_signal'] == "SELL"  # rsi_14 = 75

def test_macd_signal_generation(sample_indicator_data):
    df = sample_indicator_data.copy()
    df['macd_signal_col'] = "HOLD" # Initialize

    # Apply MACD signal logic
    df.loc[df['macd'] > df['macd_signal_data'], 'macd_signal_col'] = "BUY"
    df.loc[df['macd'] < df['macd_signal_data'], 'macd_signal_col'] = "SELL"
    
    assert df.loc[0, 'macd_signal_col'] == "HOLD"
    assert df.loc[1, 'macd_signal_col'] == "BUY" # macd (1) > macd_signal_data (0.5)
    assert df.loc[2, 'macd_signal_col'] == "SELL" # macd (-1) < macd_signal_data (-0.5)
    assert df.loc[7, 'macd_signal_col'] == "BUY" # macd (0) > macd_signal_data (-0.1)
    assert df.loc[8, 'macd_signal_col'] == "SELL" # macd (0) < macd_signal_data (0.1)


def test_bb_signal_generation(sample_indicator_data):
    df = sample_indicator_data.copy()
    df['bb_signal'] = "HOLD" # Initialize

    # Apply Bollinger Bands signal logic
    df.loc[df['Close'] < df['bb_low_band'], 'bb_signal'] = "BUY"
    df.loc[df['Close'] > df['bb_high_band'], 'bb_signal'] = "SELL"

    assert df.loc[0, 'bb_signal'] == "HOLD"  # Close (100) between bb_low (98) and bb_high (102)
    assert df.loc[1, 'bb_signal'] == "HOLD"  # Close (105) between bb_low (100) and bb_high (110)
    assert df.loc[9, 'bb_signal'] == "BUY"   # Close (90) < bb_low_band (95)
    assert df.loc[10, 'bb_signal'] == "SELL" # Close (110) > bb_high_band (105)
