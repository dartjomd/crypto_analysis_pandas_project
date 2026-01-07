import pytest
import pandas as pd

from scripts.CryptoTransformer import CryptoTransformer
from scripts.CryptoAnalyzer import CryptoAnalyzer
from scripts.enums.ColumnsToAnalyzeEnum import ColumnsToAnalyzeEnum


@pytest.fixture
def raw_api_data():
    """Create fake CoinGecko response"""
    return [
        {
            "prices": [[1700000000000, 30000.0], [1700086400000, 31000.0]],
            "total_volumes": [[1700000000000, 1000.0], [1700086400000, 1100.0]],
            "market_caps": [[1700000000000, 600000.0], [1700086400000, 620000.0]],
        }
    ]


@pytest.fixture
def coins_data():
    """Create coin/currency pair"""
    return [("bitcoin", "usd")]


def test_normalization(raw_api_data, coins_data):
    """Test that Transformer correctly creates DataFrame from JSON"""
    transformer = CryptoTransformer()
    transformer.normalize_crypto_data(data=raw_api_data, coins_data=coins_data)
    df = transformer.get_normalized_crypto()

    assert not df.empty
    # check DataFrame has all necessary columns
    assert all(
        item in df.columns
        for item in [
            "price",
            "capitalization",
            "volume",
            "coin_name",
            "currency",
            "date_key",
        ]
    )
    assert df.iloc[0]["coin_name"] == "bitcoin"
    assert isinstance(df.iloc[0]["price"], float)


def test_analyzer_volatility():
    """Test that volatility analyzer works correctly"""

    data = {
        "date_key": [20230101, 20230102],
        "price": [100.0, 110.0],
        "coin_name": ["bitcoin", "bitcoin"],
        "currency": ["usd", "usd"],
    }

    df = pd.DataFrame(data)
    analyzer = CryptoAnalyzer(df_data=df)

    res = analyzer.get_volatility(
        column=ColumnsToAnalyzeEnum.price.value,
        lag_to_row=1,
        df=df,
        coin_name="bitcoin",
        currency="usd",
    )

    assert res.iloc[0]["price_growth"] == 10.0
