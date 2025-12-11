from typing import Literal
import pandas as pd

COLUMNS_TYPE = Literal["price", "volume"]


class CryptoAnalyzer:
    """Class for executing pandas transformation to extract analyzed data from initial DataFrame"""

    def __init__(self, df_data: pd.DataFrame):
        """
        Set variables

        :param df_data: initial DataFrame for analysis containing all information
        """

        self.df = df_data

    # replace with decorator!!!
    def get_coin_currency_pair(
        self, df: pd.DataFrame, coin_name: str, currency: str
    ) -> pd.DataFrame:
        """
        Get only data for particular (coin_name, currency) pair from initial DataFrame

        :param df: initial df
        :param coin_name: name of the coin to analyze
        :param currency: currency in which conduct analyze
        :return: found DataFrame
        """

        mask = (df["coin_name"] == coin_name) & (df["currency"] == currency)
        return df[mask]

    def get_spikes(
        self,
        up_to_rank: int,
        column: COLUMNS_TYPE,
        order: Literal["DESC", "ASC"],
        coin_name: str,
        currency: str,
        start_date_key: int,
        end_date_key: int,
    ) -> pd.DataFrame:
        """
        Get days where price or volume for each (coin, currency) was either the biggest or smallest

        :param up_to_rank: amount of days
        :param column: which column to rank
        :param order: in which column order to get data
        :param coin_name: coin name to retrieve data for
        :param currency: currency in which retrieve data in
        :param start_date_key: YYYYMMDD format string for defining starting date for getting spikes
        :param end_date_key: YYYYMMDD format string for defining ending date for getting spikes
        """

        # form order argument
        sorting_order = True if order == "ASC" else False

        # copy df so it doesn't affect the initial one
        df = self.df.copy()

        df = self.get_coin_currency_pair(df, coin_name, currency)  # !!!!

        # get only those rows between dates
        date_mask = (df["date_key"] >= start_date_key) & (
            df["date_key"] <= end_date_key
        )
        df = df[date_mask]

        # sort values by given column and given order
        df = df.sort_values(by=column, ascending=sorting_order)

        # get top N rows
        df = df[:up_to_rank]
        return df

    def get_moving_average(
        self,
        column: COLUMNS_TYPE,
        total_day_span: int,
        coin_name: str,
        currency: str,
    ) -> pd.DataFrame:
        """
        Get moving average for price or volume for each (coin, currency)

        :param coin_name: coin name to retrieve data for
        :param currency: currency in which retrieve data in
        """

        # copy df so it doesn't affect the initial one
        df = self.df.copy()

        df = self.get_coin_currency_pair(df, coin_name, currency)  # !!!!

        # sort by date and get moving average by given days argument
        column_name = f"moving_avg_{column}"
        df[column_name] = (
            df.sort_values(by="date_key")
            .rolling(window=total_day_span, center=True)[column]
            .mean()
        )

        # drop non calculated rows
        df = df.dropna()
        return df

    def get_volatility(
        self, column: COLUMNS_TYPE, lag_to_row: int, coin_name: str, currency: str
    ) -> pd.DataFrame:
        """
        Get volatility by days for (coin, currency) pair

        :param column: column to analyze
        :param lag_to_row: how many days to LAG back
        :param coin_name: coin name to retrieve data for
        :param currency: currency in which retrieve data in
        """

        # copy df so it doesn't affect the initial one
        df = self.df.copy()

        df = self.get_coin_currency_pair(df, coin_name, currency)  # !!!!

        # calculate volatility based on arguments
        volatility_column = f"{column}_growth"
        df = (
            df.sort_values(by="date_key", ascending=True)
            .assign(
                previous=lambda x: x[column].shift(
                    periods=lag_to_row
                ),  # temporary previous column value based on lag_to_row argument
                growth=lambda x: round(
                    (x[column] - x["previous"]) / x["previous"] * 100,
                    2,  # calculate final growth result
                ),
            )
            .drop(columns=["previous"])  # remove temporary column
            .dropna()  # first N rows will be NaN as they dont have enough prevous values
            .rename(
                columns={"growth": volatility_column}
            )  # rename growth column to correct name
        )

        return df

    def get_monthly_analysis(self, coin_name: str, currency: str) -> pd.DataFrame:
        """
        Get monthly analysis of price and volume for (coin, currency) pair

        :param coin_name: coin name to retrieve data for
        :param currency: currency in which retrieve data in
        """

        # copy df so it doesn't affect the initial one
        df = self.df.copy()

        # change date_key type to str to operate it
        df["date_key"] = df["date_key"].astype(str)

        df = self.get_coin_currency_pair(df, coin_name, currency)  # !!!!

        # add month column to aggregate and aggregate averave values for columns
        df = (
            df.assign(
                year_month_key=lambda x: (
                    x["date_key"].astype(str).str[0:4]
                    + "-"
                    + x["date_key"].astype(str).str[4:6]
                )
            )
            .groupby(by="year_month_key", as_index=False)
            .agg(
                avg_price=("price", "mean"),
                avg_volume=("volume", "mean"),
                avg_capitalization=("capitalization", "mean"),
                coin_name=("coin_name", "first"),
                currency=("currency", "first"),
                date_key=("date_key", "first"),
            )
        )

        return df
