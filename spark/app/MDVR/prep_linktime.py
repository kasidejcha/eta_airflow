import pymongo
import pandas as pd
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import redis
import os
import warnings
warnings.filterwarnings("ignore")


def load_mongo(time_path, limit_rows):
    with open(time_path) as f:
        line = f.readlines()
    myclient = pymongo.MongoClient("mongodb://192.168.242.74:27017")
    mydb = myclient.TSB
    mycol = mydb["MDVR"]
    if len(line) == 0:
        i = mycol.find().sort('gpsTime').limit(limit_rows)
    else:
        time = line[0]
        i = mycol.find({'gpsTime':{'$gte': time}}).sort('gpsTime').limit(limit_rows)
    df = pd.DataFrame(i)
    return df


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
    threshold = (pd.to_datetime(c.max())-timedelta(minutes=15)).dt.strftime("%Y-%m-%d %H:%M:%S").iloc[0]
    c = c[c.link_timestamp > threshold].sort_values('link_timestamp').reset_index()
    c = c.iloc[0]
    with open(time_path, 'w') as f:
        f.write(c.link_timestamp)



if __name__ == '__main__':
    
    time_path = '/usr/local/spark/resources/data/tmp/mdvr_data/mdvr_time.txt'
    df = load_mongo(time_path, limit_rows=100_000)
    if len(df) > 0:
        link = find_link_time(df)
        print(link[['master_link', 'link_time', 'PlateNo', 'link_timestamp', 'upload_time']].head())
        if len(link)>0:
            file_path = "/usr/local/spark/resources/data/tmp/mdvr_data/mdvr_link.csv"
            if os.path.exists(file_path):
                # filtered
                old_link = pd.read_csv(file_path)
                link = link[~link.id.isin(old_link.id.tolist())]
                if len(link)>0:
                    link = link.sort_values('link_timestamp').reset_index(drop=True)
                    link_timestamp_fn(link, time_path)
                    
                    # save data
                    link.to_csv(file_path, index=False, encoding='utf-8-sig')
                    
                    # upload to postgresql
                    engine = create_engine("postgresql://admin:admin@192.168.14.91:5432/eta")
                    db = scoped_session(sessionmaker(bind=engine))
                    link.to_sql('mdvr_link_time', engine, if_exists='append', index=False)
                    print('Upload link time complete')
                else:
                    print('No new link time: filter old link')
            else:
                link = link.sort_values('link_timestamp').reset_index(drop=True)
                link_timestamp_fn(link, time_path)
                
                # save data
                link.to_csv(file_path, index=False, encoding='utf-8-sig')
                
                # upload to postgresql
                engine = create_engine("postgresql://admin:admin@192.168.14.91:5432/eta")
                db = scoped_session(sessionmaker(bind=engine))
                link.to_sql('mdvr_link_time', engine, if_exists='append', index=False)
                print('Upload link time complete')
        else:
            print('No new link time: find_link_time')
    else:
        print('No new link time: df')