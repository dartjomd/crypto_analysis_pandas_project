import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import time
import os
import pandas as pd
from CryptoExtracter import CryptoExtracter
from CryptoTransformer import CryptoTransformer
from CryptoVisualizer import CryptoVisualizer
from CryptoAnalyzer import CryptoAnalyzer


DAYS_OF_HISTORY = 10000
COINS = ["hh", "ethereum"]
CURRENCY = ["usd"]


def get_coins_data(
    coins_list: list[str], currency_list: list[str]
) -> list[tuple[str, str]]:
    # get coins data by generating cortesion product of coin name and currency
    return [(coin, currency) for coin in coins_list for currency in currency_list]


async def main():
    # get coins data for extracting and transforming data correctly
    coins_data = get_coins_data(coins_list=COINS, currency_list=CURRENCY)
    end_point_timestamp = int(time.time())
    start_date = datetime.now() - timedelta(days=DAYS_OF_HISTORY)
    start_timestamp = int(start_date.timestamp())

    # extract data using API
    extracter = CryptoExtracter()
    crypto_data = await extracter.get_retrospective_data(
        starting_from_timestamp=start_timestamp,
        up_to_timestamp=end_point_timestamp,
        coins_data=coins_data,
    )
    print(crypto_data)  # fix it [{}, {}]
    return
    if len(crypto_data) == 0:
        print("No data to analyse")
        return

    # transform data to DataFrame
    transformer = CryptoTransformer()
    transformer.normalize_crypto_data(data=crypto_data, coins_data=coins_data)

    # save CSV into data folder
    filename = "normalized_dataframe.csv"
    transformer.save_normalized_data_to_csv(filename)

    df_crypto = transformer.get_normalized_crypto()

    # analyse data
    analyzer = CryptoAnalyzer(df_data=df_crypto)

    # go through every (coin_name, currency) pair and save visualised data as images
    for coin, currency in coins_data:

        # visualize spikes
        start_date_key = 20251110
        end_date_key = 20251125
        df_spikes_data = analyzer.get_spikes(
            up_to_rank=5,
            order="DESC",
            column="capitalization",
            coin_name=coin,
            currency=currency,
            start_date_key=start_date_key,
            end_date_key=end_date_key,
        )
        CryptoVisualizer.plot_spikes(
            df=df_spikes_data,
            column="capitalization",
            start_date_key=start_date_key,
            end_date_key=end_date_key,
        )

        # visualize general information about price and volume
        CryptoVisualizer.plot_general_info(
            df=df_crypto, coin_name=coin, currency=currency
        )

        # visualize monthly statistics for price and volume
        df_monthly_data = analyzer.get_monthly_analysis(
            coin_name=coin, currency=currency
        )
        CryptoVisualizer.plot_monthly_analysis(df=df_monthly_data, column="avg_price")
        CryptoVisualizer.plot_monthly_analysis(df=df_monthly_data, column="avg_volume")
        CryptoVisualizer.plot_monthly_analysis(
            df=df_monthly_data, column="avg_capitalization"
        )

        # visualize monthly share of volume
        CryptoVisualizer.plot_monthly_volume_share(df=df_monthly_data, total_months=12)

        # visualize moving average
        total_day_span = 7
        df_moving_average_data = analyzer.get_moving_average(
            total_day_span=total_day_span,
            column="price",
            coin_name=coin,
            currency=currency,
        )
        CryptoVisualizer.plot_moving_average(
            df=df_moving_average_data, column="price", total_day_span=total_day_span
        )

        # visualize growth
        days_to_lag = 3
        df_volatility_data = analyzer.get_volatility(
            column="price", lag_to_row=days_to_lag, coin_name=coin, currency=currency
        )
        CryptoVisualizer.plot_volatility(
            df=df_volatility_data, column="price", days_to_lag=days_to_lag
        )
        continue


if __name__ == "__main__":
    asyncio.run(main())


# api errors, decorator
