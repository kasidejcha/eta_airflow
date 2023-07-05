import re
import pymongo
import pandas as pd
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import sqlalchemy
import redis
import os
import warnings
warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)

def load_mongo(time_path, limit_rows, mongo_url):
    with open(time_path) as f:
        line = f.readlines()
    myclient = pymongo.MongoClient(mongo_url)
    mydb = myclient.TSB
    mycol = mydb["MDVR"]
    if len(line) == 0:
        i = mycol.find().sort('gpsTime').limit(limit_rows)
        df = pd.DataFrame(i)
        df['gpsTime'] = df['gpsTime'].apply(convert_datetime)
    else:
        time = line[0]
        i = mycol.find({'gpsTime':{'$gte': time}}).sort('gpsTime').limit(limit_rows)
        df = pd.DataFrame(i)
        df['gpsTime'] = df['gpsTime'].apply(convert_datetime)
        df = df[pd.to_datetime(df.gpsTime) >= pd.to_datetime(time)]
        print(f'time: {time}')
    return df

def convert_datetime(x):
    pattern = r"(\w+\s\w+\s\d+\s\d{4}\s\d{2}:\d{2}:\d{2})"
    match = re.search(pattern, x)
    if match:
        extracted_datetime = match.group(1)
        return pd.to_datetime(extracted_datetime).strftime("%Y-%m-%d %H:%M:%S")
    else:
        return x

def find_link_time(df):
    current_time = (datetime.now()+ timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
    df = df[df.ArriveOrLeave == 1].sort_values(by = ['PlateNo', 'gpsTime']).reset_index(drop=True)
    tmp = df[['_id', 'direction', 'gpsTime', 'routeName', 'station_num', 'stationName', 'PlateNo']]
    tmp['direction'] = tmp['direction'].replace(4,1)
    tmp['direction'] = tmp['direction'].replace(5,2)
    tmp_1 = tmp.iloc[:-1].reset_index(drop=True)
    tmp_1.columns = ['_id_1', 'direction_1', 'gpsTime_1', 'routeName_1', 'station_num_1', 'stationName_1', 'PlateNo_1']
    tmp_2 = tmp.iloc[1:].reset_index(drop=True)
    tmp_2.columns = ['_id_2', 'direction_2', 'gpsTime_2', 'routeName_2', 'station_num_2', 'stationName_2', 'PlateNo_2']
    tmp = pd.concat([tmp_1, tmp_2], axis=1)
    tmp['st_diff'] = tmp.station_num_2 - tmp.station_num_1
    
    # tmp['gpsTime_2'] = tmp['gpsTime_2'].apply(convert_datetime)
    # tmp['gpsTime_1'] = tmp['gpsTime_1'].apply(convert_datetime)
    
    tmp['time_diff'] = pd.to_datetime(tmp['gpsTime_2']) - pd.to_datetime(tmp['gpsTime_1'])
    tmp.insert(0, 'id', tmp['_id_1'].astype(str) + '_' + tmp['_id_2'].astype(str))
    filtered = tmp[(tmp.st_diff == 1) & (tmp.direction_1 == tmp.direction_2) 
                    & (tmp.routeName_1 == tmp.routeName_2) & (tmp.time_diff < timedelta(seconds=1800))
                    & (tmp.PlateNo_1 == tmp.PlateNo_2)]
    station_id_1 = filtered['stationName_1'].str.lower().str.split('-').str[0]
    station_id_2 = filtered['stationName_2'].str.lower().str.split('-').str[0]
    filtered['master_link'] = station_id_1+station_id_2
    filtered['time_diff'] = filtered['time_diff'].dt.total_seconds()
    filtered = filtered[['_id_1', '_id_2', 'master_link', 'time_diff', 'PlateNo_2', 'gpsTime_2']]
    filtered.insert(0, 'id', filtered['_id_1'].astype(str) + '_' + filtered['_id_2'].astype(str))
    filtered = filtered.drop(['_id_1', '_id_2'], axis=1)
    filtered['upload_time'] = [current_time]*len(filtered)
    filtered.columns = ['id', 'master_link', 'link_time', 'PlateNo', 'link_timestamp', 'upload_time']
    return filtered


def link_timestamp_fn(link,time_path):
    check = pd.concat([link, link.id.str.split('_',expand=True)],axis=1)
    c = pd.DataFrame(check.groupby('PlateNo')['link_timestamp'].max())
    maxTime = pd.to_datetime(c.max())
    minTime = pd.to_datetime(c.min())
    threshold = (maxTime-timedelta(minutes=15)).dt.strftime("%Y-%m-%d %H:%M:%S").iloc[0]
    c = c[c.link_timestamp > threshold].sort_values('link_timestamp').reset_index()
    c = c.iloc[0]
    with open(time_path, 'w') as f:
        f.write(c.link_timestamp)
    return threshold, maxTime.dt.strftime("%Y-%m-%d %H:%M:%S").iloc[0], minTime.dt.strftime("%Y-%m-%d %H:%M:%S").iloc[0]



if __name__ == '__main__':
    eta_db_engine_string  = "postgresql://admin:admin@192.168.14.91:5431/eta"
    time_path = '/usr/local/spark/resources/data/tmp/mdvr_data/mdvr_time.txt'
    file_path = "/usr/local/spark/resources/data/tmp/mdvr_data/mdvr_link.csv"
    mongo_url = "mongodb://147.50.147.204:27017"
    current_time = (datetime.now()+ timedelta(hours=7)).strftime('%Y-%m-%d %H:%M:%S')
    
    df = load_mongo(time_path, limit_rows=100_000, mongo_url=mongo_url)
    print(df[['gpsTime']].sort_values('gpsTime', ascending=True).head())
    print(f'max: {df["gpsTime"].max()}')
    print(f'min: {df["gpsTime"].min()}')
    if len(df) > 0:
        link = find_link_time(df)
        # print(link[['master_link', 'link_time', 'PlateNo', 'link_timestamp', 'upload_time']].sort_values('link_timestamp', ascending=True).head())
        if len(link)>0:
            link = link.sort_values('link_timestamp').reset_index(drop=True)
            threshold, maxTime, minTime = link_timestamp_fn(link, time_path)
            print(f'threshold time: {threshold}')
            print(f'maxTime time: {maxTime}')
            print(f'minTime time: {minTime}')
            
            # save data
            link.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            # upload to postgresql
            engine = create_engine(eta_db_engine_string)
            pg = pd.read_sql(f"""
            SELECT *
            FROM link_time
            WHERE link_timestamp BETWEEN CAST('{minTime}' AS timestamp) - INTERVAL '1 hour'
                                    AND CAST('{maxTime}' AS timestamp);
            """, engine)
            print(f"Before drop duplicate: {len(link)}")
            link = link[~link.id.isin(pg.id.tolist())].reset_index(drop=True)
            print(f"After drop duplicate: {len(link)}")
            link_dtypes = {
                'id': sqlalchemy.String,
                'master_link': sqlalchemy.String,
                'link_time': sqlalchemy.Float,
                'PlateNo': sqlalchemy.String,
                'link_timestamp': sqlalchemy.TIMESTAMP,
                'upload_timestamp': sqlalchemy.TIMESTAMP
            }
            link.to_sql('link_time', engine, if_exists='append', index=False, dtype = link_dtypes)
            print(f"Output: {link.sort_values('link_timestamp', ascending=True).head()}")
            print('Upload link time complete')
        else:
            print('No new link time: find_link_time')
    else:
        print('No new link time: df')