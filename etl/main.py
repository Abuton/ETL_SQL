from helpers import (
    read_data,
    fix_missing_records,
    rename_field,
    add_hour_date_fields,
    merge_dataframe,
)
from etl import get_all_feature, add_datetime_dimension_to_df


if __name__ == "__main__":
    connectivity_status_df = read_data(
        filename="connectivity_status.csv", date_column="creation_time"
    )
    orders_df = read_data()
    polling_df = read_data(filename="polling.csv", date_column="creation_time")

    # fix missing records
    orders_df = fix_missing_records(orders_df)

    # rename creationtime field
    connectivity_status_df = rename_field(
        connectivity_status_df, "connectivity_creation_time"
    )
    polling_df = rename_field(polling_df, "polling_creation_time")

    # add datetime feature
    orders_df = add_hour_date_fields(orders_df, "order_creation_time")
    polling_df = add_hour_date_fields(polling_df, "polling_creation_time")
    connectivity_status_df = add_hour_date_fields(
        connectivity_status_df, "connectivity_creation_time"
    )

    # merge dataframe - preparing data
    polling_orders_df = merge_dataframe(polling_df, orders_df)

    # add add_datetime_dimension_to_df
    polling_orders_df = add_datetime_dimension_to_df(polling_orders_df)

    df = get_all_feature(polling_orders_df, orders=orders_df[["order_id"]])

    print(df.head(10))
