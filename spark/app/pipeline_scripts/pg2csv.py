import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import time
from glob import glob
from datetime import datetime, timedelta

# init
route_path = '/usr/local/spark/resources/data/routes/routes.csv'
vehicle_path = '/usr/local/spark/resources/data/vehicles/vehicles_30-11-22.csv'
vender = 'sit'

route = pd.read_csv(route_path)
vehicle = pd.read_csv(vehicle_path)
all_route_list = route.routes.unique().tolist()

engine = create_engine("postgresql://admin:admin@192.168.14.91:5432/eta")


date = datetime.now()
date = date.strftime('%Y-%m-%d')
# date = '2022-12-21'

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