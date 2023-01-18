import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import time
from glob import glob
from datetime import datetime, timedelta
import numpy as np

# init
query = """
select *
from stations_gb
"""
engine = create_engine("postgresql://divine:dv123456@192.168.242.73:5432/tsb")
vehicle_path = '/usr/local/spark/resources/data/vehicles/vehicles_2023-01-16.csv'
vender = 'thai-star'

route = pd.read_sql(query, engine).drop('distance_km', axis=1)
vehicle = pd.read_csv(vehicle_path)
all_route_list = route.Route.unique().tolist()

engine = create_engine("postgresql://admin:admin@192.168.14.91:5432/eta")


# date = datetime.now() - timedelta(days=1)
# date = date.strftime('%Y-%m-%d')
  
date = (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")

start_time = time.time()
query = f"""
select 
    CAST(imei AS bigint) as device, 
    utc_ts + INTERVAL '7 hour' as datetime_gps,
    latitude,
    longitude,
    speed,
    odometer, 
    engine_status
from gps_log
where (utc_ts + INTERVAL '7 hour')::date = '{date}' and engine_status = 1 and data_from = '{vender}'
order by utc_ts
"""
gps = pd.read_sql(query, con=engine)

gps = gps.merge(vehicle, left_on='device', right_on='gps_imei', how='left')
gps = gps.drop(['device', 'status', 'engine_status'],axis=1)
gps = gps[gps.route.isin(all_route_list)].reset_index(drop=True)
gps.rename(columns={'datetime_gps':'time', 'latitude':'lat', 'longitude':'lon'}, inplace=True)

# tmp_route_list = gps.route.unique().tolist()
# route_list = route_list + tmp_route_list
# route_list = list(set(route_list))

gps.to_csv(f'/usr/local/spark/resources/data/gps/{vender}_gps_{date}.csv', index=False, encoding='utf-8-sig')
end_time = time.time()
print(f'{date} SUCCESS: {end_time-start_time} seconds')