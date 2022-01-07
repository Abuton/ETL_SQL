## Task 3 Explanation

The process I took to answer this question is as follows;

- get a clean data by fixing all data quality issues
  - `orders.csv`: I filled all missing values in the field 'device_id' using both forward-fill and backward-fill method
- rename creation_time field to avoid name mismatch when dataframes are joined/merged together
- added hour and date feature to all 3 dataset to allow for efficient joins of multiple dataframes
- merged polling_event data with orders data using common columns (device_id, date, and hour), to get data that represent devices that one or more order has been dispatched to and has information about the polling events that occured on such device
- the 3 periods of time (3 minutes before order creation time, 3 minutes after order creation time, 1 hour before order creation time) where generated and added to the merged dataframe using the `add_datetime_dimension_to_df` function
- different functions were declared to answer all the questions which is housed under the `get_all_feature` function

### File Organisation

- `etl.py` contains all code logic depends on `helpers.py`
- `helpers.py` contains utilities functions
- `main.py` contains code implementations and output generation depends on both `helpers.py` and `etl.py`

### How to run

1. change directory into the etl folder
2. run `pip install -r requirements.txt`
3. run `python main.py`

Thank you for your time and consideration
