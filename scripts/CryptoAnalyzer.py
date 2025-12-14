import pandas as pd

from enums.OrderEnum import OrderEnum
from enums.ColumnsToAnalyze import ColumnsToAnalyze


def get_coin_currency_pair(function):
    """
    Decorator for getting (coin_name currency) pair from class properties
    It filters self.df and passes the filtered result as the second positional argument (df)
    """

    def wrapper(self, *args, **kwargs) -> pd.DataFrame:
        # get coin_name and currency for filtering
        coin_name = kwargs.get("coin_name")
        currency = kwargs.get("currency")

        # check if filtering can be done
        if not coin_name or not currency:
            raise ValueError(
                f'Error. Function {function.__name__} is missing "coin_name" or "currency" argument'
            )

        # copy DataFrame and filter
        df = self.df.copy()
        mask = (df["coin_name"] == coin_name) & (df["currency"] == currency)
        filtered_df = df[mask]

        # form full kwargs object
        full_kwargs = kwargs.copy()
        full_kwargs["df"] = filtered_df

        # return result of passed function
        return function(self, *args, **full_kwargs)

    return wrapper


class CryptoAnalyzer:
    """Class for executing pandas transformation to extract analyzed data from initial DataFrame"""

    def __init__(self, df_data: pd.DataFrame):
        """
        Set variables

        :param df_data: initial DataFrame for analysis containing all information
        """

        self.df = df_data

    @get_coin_currency_pair
    def get_spikes(
        self,
        up_to_rank: int,
        column: ColumnsToAnalyze,
        order: OrderEnum,
        start_date_key: int,
        end_date_key: int,
        df: pd.DataFrame,
        coin_name: str,
        currency: str,
    ) -> pd.DataFrame:
        """
        Get days where price or volume for each (coin, currency) was either the biggest or smallest

        :param df: Filtered DataFrame (injected by @get_coin_currency_pair)
        :param up_to_rank: amount of days
        :param column: which column to rank
        :param order: in which column order to get data
        :param start_date_key: YYYYMMDD format string for defining starting date for getting spikes
        :param end_date_key: YYYYMMDD format string for defining ending date for getting spikes
        :param coin_name: coin name to retrieve data for (used by decorator)
        :param currency: currency in which retrieve data in (used by decorator)
        """

        # get only those rows between dates
        date_mask = (df["date_key"] >= start_date_key) & (
            df["date_key"] <= end_date_key
        )
        df = df[date_mask]

        # sort values by given column and given order
        df = df.sort_values(by=column, ascending=order)

        # get top N rows
        df = df[:up_to_rank]
        return df

    @get_coin_currency_pair
    def get_moving_average(
        self,
        column: ColumnsToAnalyze,
        total_day_span: int,
        df: pd.DataFrame,
        coin_name: str,
        currency: str,
    ) -> pd.DataFrame:
        """
        Get moving average for price or volume for each (coin, currency)

        :param df: Filtered DataFrame (injected by @get_coin_currency_pair)
        :param column: which column to calculate moving average on
        :param total_day_span: number of days for the rolling window
        :param coin_name: coin name to retrieve data for (used by decorator)
        :param currency: currency in which retrieve data in (used by decorator)
        """

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

    @get_coin_currency_pair
    def get_volatility(
        self,
        column: ColumnsToAnalyze,
        lag_to_row: int,
        df: pd.DataFrame,
        coin_name: str,
        currency: str,
    ) -> pd.DataFrame:
        """
        Get volatility by days for (coin, currency) pair

        :param df: Filtered DataFrame (injected by @get_coin_currency_pair)
        :param column: column to analyze
        :param lag_to_row: how many days to LAG back
        :param coin_name: coin name to retrieve data for (used by decorator)
        :param currency: currency in which retrieve data in (used by decorator)
        """

        # calculate volatility based on arguments
        volatility_column = f"{column}_growth"
        df = (
            df.sort_values(by="date_key", ascending=OrderEnum.ascending.value)
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

    @get_coin_currency_pair
    def get_monthly_analysis(
        self, df: pd.DataFrame, coin_name: str, currency: str
    ) -> pd.DataFrame:
        """
        Get monthly analysis of price and volume for (coin, currency) pair

        :param df: Filtered DataFrame (injected by @get_coin_currency_pair)
        :param coin_name: coin name to retrieve data for (used by decorator)
        :param currency: currency in which retrieve data in (used by decorator)
        """

        # change date_key type to str to operate it
        df["date_key"] = df["date_key"].astype(str)

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
