import pytest
import pandas as pd
from scripts.CryptoAnalyzer import CryptoAnalyzer


@pytest.fixture
def sample_dataframe():
    data = {
        "date_key": [20240101, 20240102],
        "price": [40000.0, 42000.0],
        "volume": [100000.0, 120000.0],
        "capitalization": [800000.0, 840000.0],
        "coin_name": ["bitcoin", "bitcoin"],
        "currency": ["usd", "usd"],
    }
    return pd.DataFrame(data)


def test_decorator_raises_error_on_missing_args(sample_dataframe):
    """Test that decorator raises error if arguments are missing"""

    analyzer = CryptoAnalyzer(df_data=sample_dataframe)

    with pytest.raises(ValueError, match='missing "coin_name" or "currency"'):
        analyzer.get_moving_average(
            total_day_span=3, column="price"
        )  # currency argument is absent


def test_decorator_filters_data(sample_dataframe):
    """Test that decorator actually filters data based on currency and coin name"""

    analyzer = CryptoAnalyzer(df_data=sample_dataframe)
    result = analyzer.get_moving_average(
        total_day_span=1, column="price", coin_name="bitcoin", currency="usd"
    )

    assert all(result["coin_name"] == "bitcoin")
