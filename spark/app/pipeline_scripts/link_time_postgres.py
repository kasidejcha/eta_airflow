import pandas as pd
from datetime import datetime, timedelta
import os
import argparse
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker



def get_parameters():
    parser = argparse.ArgumentParser(description='Arrival_preprocessing')
    parser.add_argument('--route_num_01', type=str, default= None, help='route number')
    parser.add_argument('--route_num_02', type=str, default= None, help='route number')
    parser.add_argument('--route_num_03', type=str, default= None, help='route number')
    args = parser.parse_args()
    return args

# init
args = get_parameters()

route_num_list = [args.route_num_01, args.route_num_02, args.route_num_03]
route_num_list = list(filter(lambda x: x is not None, route_num_list)) # drop None route number

yesterday = datetime.now() - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d')

gps_file_path = f'/usr/local/spark/resources/data/arrival/{yesterday}/arrival_time/'
# gps_file_path = f'/home/ea_admin/Documents/spark/airflow-spark/spark/resources/data/arrival/{yesterday}/arrival_time/'
gps_files = os.listdir(gps_file_path)
gps_files = [gps_files[i] for i, s in enumerate(gps_files) if s.split('_')[1] in route_num_list]
# print(gps_files)

link_file_path = f'/usr/local/spark/resources/data/arrival/{yesterday}/link_time/'
# link_file_path = f'/home/ea_admin/Documents/spark/airflow-spark/spark/resources/data/arrival/{yesterday}/link_time/'
isExist = os.path.exists(link_file_path)
if not isExist:
    os.makedirs(link_file_path)
    print("The new directory is created!")


for arrival_file in gps_files:
    d = pd.read_csv(gps_file_path + arrival_file)
    routes = d.route_num.unique().tolist()
    path_list = d.path.unique().tolist()

    for route_num in routes:
        # combine to links
        link=[]
        gps = d[d.route_num==route_num]
        # gps = gps[gps.path == 'go']

        for path in path_list:
            for gps_imei in gps.gps_imei.unique().tolist():
                df = gps[gps.gps_imei == gps_imei]
                df = df[df.path == path]
                # print(f"Current gps imei: {gps_imei}, Current path: {path}")

                for loop in df.loop_num.unique().tolist():
                    if len(link)==0:
                        link_name = []
                        link_time = []
                        link_timestamp = []
                        temp = df[df.loop_num == loop]
                        for i in range(len(temp)-1):
                            if temp.station_num.tolist()[i+1] - temp.station_num.tolist()[i] == 1:
                                link_name.append(f'{temp.station_num.tolist()[i]}_{temp.station_num.tolist()[i+1]}')
                                link_time.append((pd.to_datetime(temp.time.iloc[i+1]) - pd.to_datetime(temp.time.iloc[i])).seconds)
                                link_timestamp.append(temp.time.tolist()[i+1])
                        link = pd.DataFrame(link_name)
                        # link.rename(columns={'0':'link_name'})
                        link['link_time(sec)'] = link_time
                        link['link_timestamp'] = link_timestamp
                        link['path'] = [path]*len(link)
                        link['gps_imei'] = [gps_imei]*len(link)
                    else:
                        link_name = []
                        link_time = []
                        link_timestamp = []
                        temp = df[df.loop_num == loop]
                        for i in range(len(temp)-1):
                            if temp.station_num.tolist()[i+1] - temp.station_num.tolist()[i] == 1:
                                link_name.append(f'{temp.station_num.tolist()[i]}_{temp.station_num.tolist()[i+1]}')
                                link_time.append((pd.to_datetime(temp.time.iloc[i+1]) - pd.to_datetime(temp.time.iloc[i])).seconds)
                                link_timestamp.append(temp.time.tolist()[i+1])
                        tmp = pd.DataFrame(link_name)
                        # tmp.rename(columns={'0':'link_name'})
                        tmp['link_time(sec)'] = link_time
                        tmp['link_timestamp'] = link_timestamp
                        tmp['path'] = [path]*len(tmp)
                        tmp['gps_imei'] = [gps_imei]*len(tmp)
                        link = pd.concat([link,tmp],axis=0)
        link = link.sort_values('link_timestamp')
        link.columns = ['link_name', 'link_time', 'link_timestamp', 'path', 'gps_imei']
        link['routes'] = [route_num]*len(link)
        link['split'] = link['link_name'].str.split('_')
        link.insert(1, 'start_station_num', link['split'].apply(lambda x: x[0]))
        link.insert(2, 'end_station_num', link['split'].apply(lambda x: x[1]))
        link = link.drop('split', axis=1)


        # save files
        # print(link.head())
        link.to_csv(link_file_path + arrival_file.split('.')[0] + '_link.csv', index=False, encoding='utf-8-sig')

        # upload to postgresql
        engine = create_engine("postgresql://admin:admin@192.168.14.91:5432/eta")
        db = scoped_session(sessionmaker(bind=engine))
        link.to_sql('link_time', engine, if_exists='append', index=False)
        d.to_sql('arrival_time', engine, if_exists='append', index=False)
        db.commit()



