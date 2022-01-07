import pandas as pd
import logging
from pandas.core.frame import DataFrame
import warnings

warnings.filterwarnings("ignore")
logging.basicConfig(
    format="%(asctime)s %(levelname)s - ETL code - %(message)s", level=logging.INFO
)
logging.getLogger().setLevel(logging.INFO)


def read_data(
    path: str = "appEventProcessingDataset/dataset/",
    filename: str = "orders.csv",
    date_column: str = "order_creation_time",
) -> DataFrame:
    """Takes in file path, converts time column to a datetime object
        and returns a dataframe

    Parameters
    ----------
    path: str : The file location
         (Default value = "appEventProcessingDataset/dataset/")
    filename: str : The file name
         (Default value = "orders.csv")
    date_column: str : The datetime column name
         (Default value = "order_creation_time")

    Returns
    -------
    A dataframe
    """
    df = pd.read_csv(path + filename, parse_dates=[date_column]).drop(
        "Unnamed: 0", axis=1
    )
    logging.info("Data read successfully from")
    return df


def fix_missing_records(df: DataFrame, column: str = "device_id") -> DataFrame:
    """Accepts two arguments, impute missing records using
       forward fill and backward fill method

    Parameters
    ----------
    df: DataFrame :

    column: str :
         (Default value = "device_id")

    Returns
    -------
    A dataframe
    """
    # fixing missing device_id record in orders using the ffill method
    df[column] = df[column].fillna(method="ffill").fillna(method="bfill")
    return df


def add_hour_date_fields(df: pd.DataFrame, datetime_column: str) -> DataFrame:
    """
    Extract date features (date, hour) from a dataframe
    and adds it as a new column to the dataframe

    Parameters
    ----------
    df: pd.DataFrame : A dataframe

    datetime_column: str : A Column timestamp column

    Returns
    -------
    A dataframe
    """
    df["hour"] = df[datetime_column].dt.hour
    df["date"] = df[datetime_column].dt.date
    logging.info("Added date features to dataframe")
    return df


def merge_dataframe(
    left_df: DataFrame,
    right_df: DataFrame,
    merge_columns: list = ["device_id", "hour", "date"],
    how="inner",
) -> DataFrame:
    """
    Takes two dataframe, returns the merged dataframe
    based on the `merge_columns` value

    Parameters
    ----------
    left_df: DataFrame : The first dataframe to merge to

    right_df: DataFrame : The second dataframe to merge with

    merge_columns: list : The columns to merge on
         (Default value = ["device_id")
    "hour" : A column to join on

    "date"] : A column to join on

    how : Specifies how the dataframes is to be joined
         (Default value = "inner")

    Returns
    -------
    A dataframe
    """
    logging.info("Merged two dataframe")
    df = left_df.merge(right_df, on=merge_columns, how=how)
    logging.info(f"New shape = {df.shape}")
    return df


def rename_field(
    df: DataFrame,
    new_column_name: str,
    old_column_name: str = "creation_time",
) -> DataFrame:
    """
    Takes a dataframe and new column name, returns a dataframe after
    renaming the old column name with the new column name

    Parameters
    ----------
    df: DataFrame : A dataframe that contains a column to be renamed

    new_column_name: str : The new column name

    old_column_name: str : The column name to be changed
         (Default value = "creation_time")

    Returns
    -------
    A dataframe
    """
    # rename the creation time in df
    df = df.rename(columns={old_column_name: new_column_name})
    logging.info(f"{old_column_name} renamed to {new_column_name}")
    return df
