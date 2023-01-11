import pandas as pd
import pymongo
import math
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import numpy as np
from datetime import datetime, timedelta
import pickle

myclient = pymongo.MongoClient(r"mongodb://tsb-ro:TSB%23ro2022@147.50.147.213:27017/?directConnection=true&authMechanism=DEFAULT&authSource=tsb-fleet")
mydb = myclient["tsb-fleet"]
mycol = mydb.locations

# start_day = np.arange(1,20)

today = datetime.now() # datetime utc
yesterday = datetime.now() - timedelta(days=1)

today_date = today.strftime('%Y-%m-%d')
file_path = '/home/ea_admin/Documents/spark/airflow-spark/spark/resources/data/current_gps_date/date.pkl'
with open(file_path, "wb") as file:
    pickle.dump(today_date, file)

from_date = datetime(yesterday.year, yesterday.month, yesterday.day,17, 0, 0)
to_date = datetime(today.year, today.month, today.day, 16, 59, 59)
mongo = mycol.find({"utc_ts": {"$gte": from_date, "$lte": to_date}})
df = pd.DataFrame(mongo).sort_values('utc_ts', ascending='True')
col = ['_id',
'utc_ts',
'ext_power_status',
'wh_based_speed',
'distance_until_recharge',
'soc',
'recv_utc_ts',
'longitude',
'num_sats',
'speed',
'gpsdata_id',
'fix',
'course',
'seq',
'latitude',
'mileage',
'odometer',
'alt',
'engine_status',
'passenger_status',
'door_state',
'hdop',
'driver_track1',
'driver_track3',
'driver_track2',
'imei',
'ext_power',
'queue_time',
'data_from',
'created_at']
df.columns = col
before_drop_len = len(df)
df = df[~df.duplicated(subset=['utc_ts','recv_utc_ts', 'longitude', 'latitude', 'imei'])]
df._id = df._id.astype(str)
df.reset_index(drop=True, inplace=True)
after_drop_len = len(df)
df = df.fillna('')
print(f"Date {from_date} Drop length: {before_drop_len - after_drop_len}")
df.to_csv('tmp_gps.csv', index=False, encoding='utf-8-sig')
df = pd.read_csv('tmp_gps.csv')

engine = create_engine("postgresql://admin:admin@192.168.14.91:5432/eta")

for i in range(math.ceil(len(df)/500_000)):
    db = scoped_session(sessionmaker(bind=engine))
    tmp = df.iloc[i*500_000:(i+1)*(500_000)]
    tmp.to_sql('gps_log', engine, if_exists='append', index=False)
    db.commit()