import pandas as pd
from dateutil import parser
import re
df = pd.read_csv('/home/ea_admin/Documents/spark/airflow-spark/spark/resources/data/tmp/mdvr_data/error_mdvr_gps_time.csv')


# Create a sample Series
# series = pd.Series(["2023-05-27 04:04:43", "Sat June 27 2023 04:04:43 GMT+0700 (Indochina Time)"])

# Define a regular expression pattern to extract date and time information
pattern = r"(\w+\s\w+\s\d+\s\d{4}\s\d{2}:\d{2}:\d{2})"

# Define a function to convert the second format to the first format
def convert_datetime(x):
    match = re.search(pattern, x)
    if match:
        extracted_datetime = match.group(1)
        # print(extracted_datetime)
        return pd.to_datetime(extracted_datetime).strftime("%Y-%m-%d %H:%M:%S")
    else:
        return x

# Apply the function to the Series using apply()
# print(series.apply(convert_datetime))

c1 = pd.to_datetime(df['gpsTime_1'].apply(convert_datetime))
c2 = pd.to_datetime(df['gpsTime_2'].apply(convert_datetime))
print(c2-c1)
