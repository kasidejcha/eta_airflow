import pandas as pd
import pymssql
import time
from datetime import datetime
a = pd.date_range('2022-11-14', periods=17).date
a = [i.strftime('%Y-%m-%d') for i in a]


conn = pymssql.connect(server='119.59.114.113:1433', user='tsb', password='Gps@tsb', database="tsb")

for i in a:
    query = f"""
    SELECT *  
    FROM dbo.gps_log
    WHERE CAST(datetime AS DATE) = '{i}';
    """
    start_time = time.time()
    df = pd.read_sql_query(query, conn)
    df.to_csv(f'/home/ea_admin/Documents/spark/airflow-spark/spark/resources/data/gps_sit/sitgps_{i}.csv', index=False, encoding='utf-8-sig')
    end_time = time.time()
    print('Date:', i)
    print('Time taken:', end_time-start_time)