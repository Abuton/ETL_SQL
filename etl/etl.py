# import dependencies
import pandas as pd
import logging
import numpy as np
from pandas.core.frame import DataFrame
import warnings
from helpers import rename_field

warnings.filterwarnings("ignore")
logging.basicConfig(
    format="%(asctime)s %(levelname)s - ETL code - %(message)s", level=logging.INFO
)
logging.getLogger().setLevel(logging.INFO)


def add_datetime_dimension_to_df(df: DataFrame):
    """
    Takes a dataframe, adds three new columns to it by calculating
    the value across timestamp

    Parameters
    ----------
    df: pd.DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    df["three_minutes_b4_order_creation_time"] = df[
        "order_creation_time"
    ] - pd.to_timedelta(3, unit="m")
    df["three_minutes_after_order_creation_time"] = df[
        "order_creation_time"
    ] + pd.to_timedelta(3, unit="m")
    df["one_hour_before_order_creation_time"] = df[
        "order_creation_time"
    ] - pd.to_timedelta(1, unit="hr")
    logging.info(
        f"3 date dimension column added to dataframe: which are:: {df.columns[-3:]}"
    )
    return df


def get_3min_b4_order_creation_time_data(df: DataFrame) -> DataFrame:
    """
    Takes a dataframe, returns a subset of the dataframe containing
    polling event data that occurs 3 minutes before an order creation time

    Parameters
    ----------
    df: pd.DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    three_minute_b4_order_creation_time = df[
        (
            (df["order_creation_time"] >= df["polling_creation_time"])
            & (
                df["polling_creation_time"]
                >= df["three_minutes_b4_order_creation_time"]
            )
        )
    ]
    return three_minute_b4_order_creation_time


def get_3min_after_order_creation_time_data(df: DataFrame) -> DataFrame:
    """
    Takes a dataframe, returns a subset of the dataframe containing
    polling event data that occurs 3 minutes after an order creation time

    Parameters
    ----------
    df: pd.DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    three_minute_after_order_creation_time = df[
        (
            (df["order_creation_time"] <= df["polling_creation_time"])
            & (
                df["polling_creation_time"]
                <= df["three_minutes_after_order_creation_time"]
            )
        )
    ]
    return three_minute_after_order_creation_time


def get_1hr_b4_order_creation_time_data(df: DataFrame) -> DataFrame:
    """
    Takes a dataframe, returns a subset of the dataframe containing
    polling event data that occurs 1 hour before an order creation time

    Parameters
    ----------
    df: pd.DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    one_hr_b4_order_creation_time = df[
        (
            (df["order_creation_time"] >= df["polling_creation_time"])
            & (df["polling_creation_time"] >= df["one_hour_before_order_creation_time"])
        )
    ]
    return one_hr_b4_order_creation_time


# get polling events counts


def get_total_count_polling_event_3min_b4_order_creation_time(
    df: DataFrame,
) -> DataFrame:
    """Takes a dataframe, filter the data by selecting polling events 3 minutes
    less than the order time, performs a groupby selecting each order
    information, and returns a dataframe with total count of polling events
    that occured 3 minutes before an order is dispatched

    Parameters
    ----------
    df: DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    df_3min_b4 = get_3min_b4_order_creation_time_data(df)
    three_minute_b4_order_creation_time = (
        df_3min_b4.groupby("order_id")["order_id"]
        .count()
        .reset_index(name="total_polling_event_three_minute_b4")
        .sort_values(by="total_polling_event_three_minute_b4", ascending=False)
        .reset_index(drop=True)
    )
    return three_minute_b4_order_creation_time


def get_total_count_polling_event_3min_after_order_creation_time(
    df: DataFrame,
) -> DataFrame:
    """Takes a dataframe, filter the data by selecting polling events 3 minutes
    after the order time, performs a groupby selecting each order
    information, and returns a dataframe with total count of polling events
    that occured 3 minutes after an order is dispatched

    Parameters
    ----------
    df: DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    df_3min_after = get_3min_after_order_creation_time_data(df)
    three_minute_after_order_creation_time = (
        df_3min_after.groupby("order_id")["order_id"]
        .count()
        .reset_index(name="total_polling_event_three_minute_after")
        .sort_values(by="total_polling_event_three_minute_after", ascending=False)
        .reset_index(drop=True)
    )
    return three_minute_after_order_creation_time


def get_total_count_polling_event_1hr_b4_order_creation_time(
    df: DataFrame,
) -> DataFrame:
    """Takes a dataframe, filter the data by selecting polling events 1 hour
    less than the order time, performs a groupby selecting each order
    information, and returns a dataframe with total count of polling events
    that occured 1 hour before an order is dispatched

    Parameters
    ----------
    df: DataFrame :

    Returns
    -------
    A dataframe
    """
    one_hr_b4 = get_1hr_b4_order_creation_time_data(df)
    one_hr_b4_order_creation_time = (
        one_hr_b4.groupby("order_id")["order_id"]
        .count()
        .reset_index(name="total_polling_event_one_hr_b4")
        .sort_values(by="total_polling_event_one_hr_b4", ascending=False)
        .reset_index(drop=True)
    )
    return one_hr_b4_order_creation_time


# the count of each type of polling status code


def get_total_count_polling_status_code_3min_b4_order_creation_time(
    df: DataFrame,
) -> DataFrame:
    """Takes a dataframe, filter the data by selecting polling events 3 minutes
    less than the order time, performs a groupby selecting each order
    information, and returns a dataframe with total count of polling statuscode
    that occured 3 minutes before an order is dispatched

    Parameters
    ----------
    df: DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    df_3min_b4 = get_3min_b4_order_creation_time_data(df)
    polling_status_code_count = (
        df_3min_b4.groupby("order_id")["status_code"]
        .value_counts()
        .reset_index(name="status_code_count")
        .sort_values("status_code_count", ascending=False)
        .reset_index(drop=True)
    )
    return polling_status_code_count


def get_total_count_polling_status_code_3min_after_order_creation_time(
    df: DataFrame,
) -> DataFrame:
    """Takes a dataframe, filter the data by selecting polling events 3 minutes
    after the order time, performs a groupby selecting each order
    information, and returns a dataframe with total count of polling statuscode
    that occured 3 minutes after an order is dispatched

    Parameters
    ----------
    df: DataFrame :

    Returns
    -------
    A dataframe
    """
    df_3min_after = get_3min_after_order_creation_time_data(df)
    polling_status_code_count = (
        df_3min_after.groupby("order_id")["status_code"]
        .value_counts()
        .reset_index(name="status_code_count_3min_after_order_creation_time")
        .sort_values(
            "status_code_count_3min_after_order_creation_time", ascending=False
        )
        .reset_index(drop=True)
    )
    return polling_status_code_count


def get_total_count_polling_status_code_1hr_before_order_creation_time(
    df: DataFrame,
) -> DataFrame:
    """Takes a dataframe, filter the data by selecting polling events 1 hour
    before the order time, performs a groupby selecting each order
    information, and returns a dataframe with total count of polling statuscode
    that occured 1 hour before an order is dispatched

    Parameters
    ----------
    df: DataFrame :

    Returns
    -------
    A dataframe
    """
    one_hr_b4 = get_1hr_b4_order_creation_time_data(df)
    polling_status_code_count = (
        one_hr_b4.groupby("order_id")["status_code"]
        .value_counts()
        .reset_index(name="status_code_count_one_hr_b4_order_creation_time")
        .sort_values("status_code_count_one_hr_b4_order_creation_time", ascending=False)
        .reset_index(drop=True)
    )
    return polling_status_code_count


# count of each type of error_code and count of responses without error codes


def get_count_error_code_3min_b4_order_creation_time(df: DataFrame) -> DataFrame:
    """Takes a dataframe, filter the data by selecting polling events 3 minutes
    less than the order time, performs a groupby selecting each order
    information, and returns a dataframe with total count of polling error_code
    that occured 3 minutes before an order is dispatched

    Parameters
    ----------
    df: DataFrame :

    Returns
    -------
    A dataframe
    """
    df_3min_b4 = get_3min_b4_order_creation_time_data(df)
    error_code_count = (
        df_3min_b4.groupby("order_id")["error_code"]
        .value_counts()
        .reset_index(name="error_code_count_3min_b4_order_creation_time")
        .sort_values("error_code_count_3min_b4_order_creation_time", ascending=False)
        .reset_index(drop=True)
    )
    return error_code_count


def get_count_error_code_3min_after_order_creation_time(df: DataFrame) -> DataFrame:
    """Takes a dataframe, filter the data by selecting polling events 3 minutes
    after the order time, performs a groupby selecting each order
    information, and returns a dataframe with total count of polling error_code
    that occured 3 minutes after an order is dispatched

    Parameters
    ----------
    df: DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    df_3min_after = get_3min_after_order_creation_time_data(df)
    error_code_count = (
        df_3min_after.groupby("order_id")["error_code"]
        .value_counts()
        .reset_index(name="error_code_count_3min_after_order_creation_time")
        .sort_values("error_code_count_3min_after_order_creation_time", ascending=False)
        .reset_index(drop=True)
    )
    return error_code_count


def get_count_error_code_1hr_b4_order_creation_time(df: DataFrame) -> DataFrame:
    """Takes a dataframe, filter the data by selecting polling events 1 hour
    before the order time, performs a groupby selecting each order
    information, and returns a dataframe with total count of polling error_code
    that occured 1 hour before an order is dispatched

    Parameters
    ----------
    df: DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    df_1hr_b4 = get_1hr_b4_order_creation_time_data(df)
    error_code_count = (
        df_1hr_b4.groupby("order_id")["error_code"]
        .value_counts()
        .reset_index(name="error_code_count_1hr_b4_order_creation_time")
        .sort_values("error_code_count_1hr_b4_order_creation_time", ascending=False)
        .reset_index(drop=True)
    )
    return error_code_count


# error code count


def get_no_error_code_data(df: DataFrame) -> DataFrame:
    """Takes a dataframe, returns a subset of the dataframe
    where error_code is NaN by selecting the missing rows and
    filling with `NOERRORRESPONSE`

    Parameters
    ----------
    df: DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    no_error_codes = df[df["error_code"].isna()]
    no_error_codes["error_code"] = no_error_codes["error_code"].fillna(
        "NOERRORRESPONSE"
    )
    return no_error_codes


def get_count_response_no_error_code_3min_b4_order_creation_time(
    df: DataFrame,
) -> DataFrame:
    """Takes a dataframe, returns a dataframe with count of response
    with no error codes 3 minutes before order creation time

    Parameters
    ----------
    df: DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    no_error_codes = get_no_error_code_data(df)
    no_error_code_3min_b4 = get_count_error_code_3min_b4_order_creation_time(
        no_error_codes
    )
    no_error_code_3min_b4 = rename_field(
        no_error_code_3min_b4,
        "no_error_code_count_3min_b4_order_creation_time",
        "error_code_count_3min_b4_order_creation_time",
    )
    return no_error_code_3min_b4


def get_count_response_no_error_code_3min_after_order_creation_time(
    df: DataFrame,
) -> DataFrame:
    """Takes a dataframe, returns a dataframe with count of response
    with no error codes 3 minutes after order creation time

    Parameters
    ----------
    df: DataFrame : A dataframe

    Returns
    -------
    A dataframe
    """
    no_error_codes = get_no_error_code_data(df)
    no_error_code_3min_after = get_count_error_code_3min_after_order_creation_time(
        no_error_codes
    )
    no_error_code_3min_after = rename_field(
        no_error_code_3min_after,
        "no_error_code_count_3min_after_order_creation_time",
        "error_code_count_3min_after_order_creation_time",
    )
    return no_error_code_3min_after


def get_count_response_no_error_code_1hr_b4_order_creation_time(
    df: DataFrame,
) -> DataFrame:
    """Takes a dataframe, returns a dataframe with count of response
    with no error codes 1 hour before order creation time

    Parameters
    ----------
    df: DataFrame : A  dataframe

    Returns
    -------
    A dataframe
    """
    no_error_codes = get_no_error_code_data(df)
    no_error_code_1hr = get_count_error_code_1hr_b4_order_creation_time(no_error_codes)
    no_error_code_1hr = rename_field(
        no_error_code_1hr,
        "no_error_code_count_1hr_b4_order_creation_time",
        "error_code_count_1hr_b4_order_creation_time",
    )
    return no_error_code_1hr


# implement all function defined here


def get_all_feature(df: DataFrame, orders: DataFrame) -> DataFrame:
    """Takes a dataframe and a series of order_id, implements all functions
    on a higher abstraction level and saves a csv at current directory.

    Parameters
    ----------
    df: DataFrame :

    orders: DataFrame :

    Returns
    -------
    A dataframe
    """
    # total count of all polling events
    total_count_polling_event_3min_b4 = (
        get_total_count_polling_event_3min_b4_order_creation_time(df)
    )
    total_count_polling_event_3min_after = (
        get_total_count_polling_event_3min_after_order_creation_time(df)
    )
    total_count_polling_event_1hr_b4 = (
        get_total_count_polling_event_1hr_b4_order_creation_time(df)
    )
    # count of each type of polling status_code
    total_count_polling_status_code_3min_b4 = (
        get_total_count_polling_status_code_3min_b4_order_creation_time(df)
    )
    total_count_polling_status_code_3min_after = (
        get_total_count_polling_status_code_3min_after_order_creation_time(df)
    )
    total_count_polling_status_code_1hr_b4 = (
        get_total_count_polling_status_code_1hr_before_order_creation_time(df)
    )
    # The count of each type of polling error_code
    total_count_polling_error_code_3min_b4 = (
        get_count_error_code_3min_b4_order_creation_time(df)
    )
    total_count_polling_error_code_3min_after = (
        get_count_error_code_3min_after_order_creation_time(df)
    )
    total_count_polling_error_code_1hr_b4 = (
        get_count_error_code_1hr_b4_order_creation_time(df)
    )
    # The count of response without error code
    total_count_polling_no_error_code_3min_b4 = (
        get_count_response_no_error_code_3min_b4_order_creation_time(df)
    )
    total_count_polling_no_error_code_3min_after = (
        get_count_response_no_error_code_3min_after_order_creation_time(df)
    )
    total_count_polling_no_error_code_1hr_b4 = (
        get_count_response_no_error_code_1hr_b4_order_creation_time(df)
    )

    polling_events = total_count_polling_event_3min_b4.merge(
        total_count_polling_event_3min_after, on="order_id"
    ).merge(total_count_polling_event_1hr_b4, on="order_id")
    status_code_count = total_count_polling_status_code_3min_b4.merge(
        total_count_polling_status_code_3min_after, on=["order_id", "status_code"]
    ).merge(total_count_polling_status_code_1hr_b4, on=["order_id", "status_code"])
    error_code_count = total_count_polling_error_code_3min_b4.merge(
        total_count_polling_error_code_3min_after, on=["order_id", "error_code"]
    ).merge(total_count_polling_error_code_1hr_b4, on=["order_id", "error_code"])
    no_error_code_count = total_count_polling_no_error_code_3min_b4.merge(
        total_count_polling_no_error_code_3min_after, on=["order_id", "error_code"]
    ).merge(total_count_polling_no_error_code_1hr_b4, on=["order_id", "error_code"])
    error_codes = error_code_count.merge(
        no_error_code_count, on=["order_id", "error_code"]
    )

    main_data = (
        orders.merge(polling_events, how="left", on="order_id")
        .merge(status_code_count, how="left", on="order_id")
        .merge(error_codes, how="left", on="order_id")
    )
    main_data.replace(np.nan, 0, inplace=True)
    main_data.sort_values("order_id", inplace=True)
    df.to_csv("input_data.csv", index=False)
    main_data.to_csv("output_data.csv", index=False)
    return main_data
