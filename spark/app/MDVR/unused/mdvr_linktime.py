import pymongo
import pandas as pd
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import os
import warnings
warnings.filterwarnings("ignore")



def find_link_time(df, link_data):
    df = df[df.ArriveOrLeave == 1].sort_values(by = ['PlateNo', 'gpsTime']).reset_index(drop=True)
    tmp = df[['_id', 'direction', 'gpsTime', 'routeName', 'station_num', 'PlateNo']]
    tmp_1 = tmp.iloc[:-1].reset_index(drop=True)
    tmp_1.columns = ['_id_1', 'direction_1', 'gpsTime_1', 'routeName_1', 'station_num_1', 'PlateNo_1']
    tmp_2 = tmp.iloc[1:].reset_index(drop=True)
    tmp_2.columns = ['_id_2', 'direction_2', 'gpsTime_2', 'routeName_2', 'station_num_2', 'PlateNo_2']
    tmp = pd.concat([tmp_1, tmp_2], axis=1)
    tmp['st_diff'] = tmp.station_num_2 - tmp.station_num_1
    tmp['time_diff'] = pd.to_datetime(tmp['gpsTime_2']) - pd.to_datetime(tmp['gpsTime_1'])
    tmp.insert(0, 'id', tmp['_id_1'].astype(str) + '_' + tmp['_id_2'].astype(str))
    filtered = tmp[(tmp.st_diff == 1) & (tmp.direction_1 == tmp.direction_2) 
                    & (tmp.routeName_1 == tmp.routeName_2) & (tmp.time_diff < timedelta(seconds=1800))
                    & (tmp.PlateNo_1 == tmp.PlateNo_2)]

    link_time = filtered[['id', 'direction_1', 'gpsTime_2', 'time_diff','routeName_1', 'station_num_1', 'station_num_2', 'PlateNo_1']]
    link_time.columns = ['id', 'direction', 'gpsTime', 'link_time', 'routeName', 'start_station_num', 'end_station_num', 'PlateNo']
    link_time.link_time = link_time.link_time.dt.seconds
    link_time['direction'] = link_time['direction'].replace(1, 'G')
    link_time['direction'] = link_time['direction'].replace(2, 'B')

    link = pd.merge(link_time, link_data, left_on=['routeName', 'direction', 'start_station_num', 'end_station_num'], 
            right_on=['Route', 'Direction', 'station_num_1', 'station_num_2'])
    link = link[['id','master_link', 'start_station_num', 'end_station_num', 'routeName','direction', 'PlateNo', 'link_time','gpsTime']]
    link.columns = ['id','master_link', 'start_station_num', 'end_station_num', 'routes','direction', 'PlateNo', 'link_time','link_timestamp']

    if len(link) != 0:
        check = pd.concat([link, link.id.str.split('_', expand=True)],axis=1)
        c = pd.DataFrame(check.groupby('PlateNo')['link_timestamp'].max())
        threshold = (pd.to_datetime(c.max())-timedelta(minutes=30)).dt.strftime("%Y-%m-%d %H:%M:%S").iloc[0]
        c = c[c.link_timestamp > threshold].sort_values('link_timestamp').reset_index()
        c = c.iloc[0]
        obj_id = check[(check.PlateNo == c.PlateNo) & (check.link_timestamp == c.link_timestamp)][0].tolist()[0]
        return link, obj_id, c.link_timestamp
    else:
        return link, link, link


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


def link_timestamp(link,time_path):
    check = pd.concat([link, link.id.str.split('_',expand=True)],axis=1)
    c = pd.DataFrame(check.groupby('PlateNo')['link_timestamp'].max())
    threshold = (pd.to_datetime(c.max())-timedelta(minutes=15)).dt.strftime("%Y-%m-%d %H:%M:%S").iloc[0]
    c = c[c.link_timestamp > threshold].sort_values('link_timestamp').reset_index()
    c = c.iloc[0]
    with open(time_path, 'w') as f:
        f.write(c.link_timestamp)



link_data = pd.read_csv('/usr/local/spark/resources/data/tmp/mdvr_data/link_data.csv')
time_path = '/usr/local/spark/resources/data/tmp/mdvr_data/time.txt'
df = load_mongo(time_path, limit_rows=10_000)
link, obj_id, time = find_link_time(df, link_data)
if len(link) != 0:
    file_path = "/usr/local/spark/resources/data/tmp/mdvr_data/link.csv"
    if os.path.exists(file_path):
        # filtered
        old_link = pd.read_csv(file_path)
        link = link[~link.id.isin(old_link.id.tolist())]
        link['upload_time'] = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]*len(link)
        link_timestamp(link, time_path)
        # save data
        link.to_csv(file_path, index=False, encoding='utf-8-sig')
        # upload to postgresql
        engine = create_engine("postgresql://admin:admin@192.168.14.91:5432/eta")
        db = scoped_session(sessionmaker(bind=engine))
        link.to_sql('link_time_mdvr', engine, if_exists='append', index=False)
        print('Upload link time complete')
    else:
        link['upload_time'] = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")]*len(link)
        link_timestamp(link, time_path)
        # save data
        link.to_csv(file_path, index=False, encoding='utf-8-sig')
        # upload to postgresql
        engine = create_engine("postgresql://admin:admin@192.168.14.91:5432/eta")
        db = scoped_session(sessionmaker(bind=engine))
        link.to_sql('link_time_mdvr', engine, if_exists='append', index=False)
        print('Upload link time complete')
else:
    print('No new link time')