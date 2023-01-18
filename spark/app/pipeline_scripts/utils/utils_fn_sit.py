import pandas as pd
import numpy as np
from glob import glob
import os
import geopy.distance
import datetime
from tqdm import tqdm



def find_route(routes, route_num, path):
    routes_path = routes[routes.routes==route_num][routes.direction==path].reset_index(drop=True)
    routes_path = routes_path.reset_index()
    routes_path['station_num'] = routes_path[routes_path.columns[0]]
    routes_path = routes_path.drop('index', axis=1)
    station_num = routes_path['station_num'].tolist()
    route_lon = routes_path.lon.tolist()
    route_lat = routes_path.lat.tolist()

    route_lat_lon = []
    for i in range(len(route_lon)):
        tmp = {'lat':route_lat[i], 'lon':route_lon[i]}
        route_lat_lon.append(tmp)
    return route_lat_lon, station_num

def gps_lat_lon_no_date_filter(gps, route_num, gps_imei):
    gps_data = gps[gps.routes==route_num][gps.gps==gps_imei].sort_values('time').reset_index(drop=True)
    if len(gps_data) != 0:
        gps_data = gps_data.sort_values('time').reset_index(drop=True)
        gps_data['time'] = pd.to_datetime(gps_data.time).dt.strftime('%Y-%m-%d %H:%M:%S')

        # get lat lon
        gps_data_lat_lon = []
        for i in range(len(gps_data)):
            tmp = {'lat':gps_data.lat.iloc[i], 'lon':gps_data.lon.iloc[i]}
            gps_data_lat_lon.append(tmp)
            
        return gps_data_lat_lon, gps_data
    else:
        print('No vehicle contain in gps_data')
        print('')


def find_direction(station_num, route_lat_lon, gps_data_lat_lon, gps_data):
    # find start and end sequences
    # 3 stations
    sequence_length = 4
    first_seq = station_num[:sequence_length]
    seq = []
    for j in range(len(first_seq)-2):
        for k in range(len(first_seq)-1):
            for c in range(len(first_seq)):
                if c>k and k>j:
                    tmp = str(first_seq[j])+str(first_seq[k]) + str(first_seq[c])
                    seq.append(tmp)

    last_seq = station_num[-sequence_length:]
    end_seq = []
    for j in range(len(last_seq)-2):
        for k in range(len(last_seq)-1):
            for c in range(len(last_seq)):
                if c>k and k>j:
                    tmp = str(last_seq[j])+str(last_seq[k]) + str(last_seq[c])
                    end_seq.append(tmp)


    start_station = route_lat_lon[0]
    station_index = []
    start_index = []
    seq_list = []
    go_count = 0

    end_station = route_lat_lon[-1]
    end_station_index = []
    end_index = []
    end_seq_list = []
    end_go_count = 0
    find_end=0

    time_filter = 1800
    seq_index = []
    end_seq_index = []
    for i in range(len(gps_data_lat_lon)):
        coords_1 = (gps_data.lat.iloc[i],gps_data.lon.iloc[i])

        dis_list = []
        for j in range(len(station_num)):
            coords_2 = route_lat_lon[j] # first 10 stations
            dis_list.append(geopy.distance.distance(coords_1, (coords_2['lat'], coords_2['lon'])).m)
            

        dis = min(dis_list)
        closest_stop_num = np.argmin(dis_list)
        dis_list=[]


        # start
        if dis < 120 and closest_stop_num < 10:
            if len(station_index)==0: # doesn't repeat index
                station_index.append(closest_stop_num)
                seq_index.append(i)
            if len(station_index)>=1 and station_index[-1]!=closest_stop_num:
                station_index.append(closest_stop_num)
                seq_index.append(i)
                # print(station_index)

            # prevent invert lap
            # or we can use seq of 3 numbers to prevent invert lap
            # go_count_diff = i-go_count
            time_diff = pd.to_datetime(gps_data.time.iloc[i]) - pd.to_datetime(gps_data.time.iloc[go_count])
            go_count = i
            # if go_count_diff >= 80:
                # station_index=[]
                # continue

            time_limit = datetime.timedelta(0,time_filter,0) # 40 mins
            if time_diff < time_limit:
                go_count_diff = 1
            else:
                go_count_diff = 100
                station_index=[]
                seq_index=[]
                continue

            if len(station_index)==3 and go_count_diff == 1:
                # print('seq:', station_index)
                s = ''.join(str(i) for i in station_index) # for matching, turn them to strings strings
                station_index=[]
                start_i = seq_index[0]
                seq_index=[]
                if (s in seq): # stations must be in sequence Ex.(12),(23)
                    diff = []
                    diff_pos = []
                    seq_list.append(s)
                    for k in range(100): # look at points around itself to find closest index to starting station
                        m=100
                        point = start_i-m+k
                        coords = (gps_data.lat.iloc[point],gps_data.lon.iloc[point])
                        diff_dis = geopy.distance.distance(coords, (start_station['lat'], start_station['lon'])).m
                        diff.append(diff_dis)
                        diff_pos.append(point)

                    diff_min = np.argmin(diff)
                    pos = diff_pos[diff_min]

                    if len(start_index) == 0:
                        start_index.append(pos)
                        find_end = 1
                    if len(start_index)>=1 and abs(start_index[-1]-(pos))>m: # 80 points --> remove near by points
                        start_index.append(pos)
                        if len(start_index) - len(end_index) == 2:
                            start_index.pop(-2)
                        find_end = 1
                    
                    diff_pos = []
                    diff = []


        # end
        if dis < 120 and closest_stop_num > station_num[-10] and find_end == 1:
            if len(end_station_index)==0: # doesn't repeat index
                end_station_index.append(closest_stop_num)
                end_seq_index.append(i)
            if len(end_station_index)>=1 and end_station_index[-1]!=closest_stop_num:
                end_station_index.append(closest_stop_num)
                end_seq_index.append(i)

            # prevent invert lap
            # or we can use seq of 3 numbers to prevent invert lap
            # go_count_diff = i-end_go_count
            time_diff = pd.to_datetime(gps_data.time.iloc[i]) - pd.to_datetime(gps_data.time.iloc[end_go_count])
            end_go_count = i
            # if go_count_diff >= 80:
            #     end_station_index=[]
            #     continue
            
            time_limit = datetime.timedelta(0,time_filter,0) # 40 mins
            if time_diff < time_limit:
                go_count_diff = 1
            else:
                go_count_diff = 100
                end_station_index=[]
                end_seq_index=[]
                continue

            if len(end_station_index)==3 and go_count_diff==1:
                # print('seq:', station_index)
                s = ''.join(str(i) for i in end_station_index) # for matching, turn them to strings strings
                end_station_index=[]
                start_i = end_seq_index[-1]
                end_seq_index=[]

                if (s in end_seq): # stations must be in sequence Ex.(12),(23)
                    diff = []
                    diff_pos = []
                    end_seq_list.append(s)
                    try:
                        for k in range(100): # look at points around itself to find closest index to starting station
                            m=100
                            point = start_i+m-k
                            coords = (gps_data.lat.iloc[point],gps_data.lon.iloc[point])
                            diff_dis = geopy.distance.distance(coords, (end_station['lat'], end_station['lon'])).m
                            diff.append(diff_dis)
                            diff_pos.append(point)
                    except:
                        point = i
                        coords = (gps_data.lat.iloc[point],gps_data.lon.iloc[point])
                        diff_dis = geopy.distance.distance(coords, (end_station['lat'], end_station['lon'])).m
                        diff.append(diff_dis)
                        diff_pos.append(point)

                    diff_min = np.argmin(diff)
                    pos = diff_pos[diff_min]

                    if len(end_index) == 0:
                        end_index.append(pos)
                        find_end = 0
                    if len(end_index)>=1 and abs(end_index[-1]-(pos))>m: # 20 points --> remove near by points
                        end_index.append(pos)
                        find_end = 0
                    
                    diff_pos = []
                    diff = []

    if len(start_index)> len(end_index):
        start_index = start_index[:-1]

    return start_index, end_index

def dis_error_check(route_lat_lon, start_index, end_index, gps_data):
    # Check for points not near first station
    start_station = route_lat_lon[0]
    start_error_index = []
    all_dis = []
    for i in range(len(start_index)):
        coords_1 = (gps_data.lat.iloc[start_index[i]],gps_data.lon.iloc[start_index[i]])
        dis = geopy.distance.distance(coords_1, (start_station['lat'], start_station['lon'])).m
        all_dis.append(dis)
        if dis > 120:
            print(dis)
            start_error_index.append(i)
            print('start_error:',start_index[i])

    # Check for points not near last station
    end_station = route_lat_lon[-1]
    end_error_index = []
    all_dis = []
    for i in range(len(end_index)):
        coords_1 = (gps_data.lat.iloc[end_index[i]],gps_data.lon.iloc[end_index[i]])
        dis = geopy.distance.distance(coords_1, (end_station['lat'], end_station['lon'])).m
        all_dis.append(dis)
        if dis > 120:
            print(dis)
            end_error_index.append(i)
            print('end_error:',end_index[i])

def filter_loop(start_index, end_index, gps_data, time_hour):
    print("Before filter length:", len(start_index))
    start = start_index.copy()
    end = end_index.copy()
    time_diff=[]
    for i in range(len(start)):
        time_diff.append(pd.to_datetime(gps_data.time.iloc[end[i]]) - pd.to_datetime(gps_data.time.iloc[start[i]]))
    start_end_df = pd.DataFrame(start)
    start_end_df.rename(columns={0:'start'}, inplace=True)
    start_end_df['end'] = end
    start_end_df['diff'] = start_end_df['end']-start_end_df['start']
    start_end_df['time_diff'] = time_diff
    start_end_df = start_end_df[start_end_df.start >= 0]
    time_limit = start_end_df.time_diff.mean()+ 3*start_end_df.time_diff.std()
    start_end_df = start_end_df[start_end_df['time_diff']<time_limit] # remove outliner
    # remove double loop
    time_limit = datetime.timedelta(0,time_hour*60*60,0)
    # time_limit = start_end_df.time_diff.mean()+ 3*start_end_df.time_diff.std()
    start_end_df = start_end_df[start_end_df['time_diff']<time_limit]
    
    print("After filter length:", len(start_end_df))
    if len(start_end_df) > 0:
        print("Max loop time:", max(start_end_df.time_diff))
        print("Min loop time:", min(start_end_df.time_diff))
        print("Median loop time:", start_end_df.time_diff.median())
    return start_end_df

def distance(lat1, lon1, lat2, lon2):
    dis = geopy.distance.distance((lat1,lon1), (lat2,lon2)).m
    return dis

def closest(data, v):
    min_station = min(data, key=lambda p: distance(v['lat'],v['lon'],p['lat'],p['lon']))
    return distance(min_station['lat'],min_station['lon'], v['lat'], v['lon']), min_station

def find_closest_distance(start, end, k, route_lat_lon, gps_data, gps_data_lat_lon):
    start = start
    end = end+k
    loop = gps_data_lat_lon[start:end]
    all_dis=[]
    all_station=[]
    time=[]
    for i, v in enumerate(loop):
        dis, station = closest(route_lat_lon, v)
        all_dis.append(dis)
        all_station.append(station)
        time.append(gps_data.time.values[start+i])

    count = 0
    for i in [all_dis, time, all_station]:
        if count == 0:
            df = pd.DataFrame(i)
            count += 1
        else:
            tmp = pd.DataFrame(i)
            df = pd.concat([df,tmp],axis=1)

    df.columns = ['distance', 'time', 'station_lat','station_long']

    stations = pd.DataFrame(route_lat_lon).reset_index()
    stations.columns = ['station_num','lat','long']
    station_list=[]
    for i in range(len(df)):
        for j in range(len(stations)):
            if df.station_lat.iloc[i] == stations.lat.iloc[j] and df.station_long.iloc[i] == stations.long.iloc[j]:
                station_list.append(stations.station_num.iloc[j])
    # debug
    # print('length df:', len(df))
    # print('length station_num:', len(station_list))
    # print(df.head())
    # print(station_list)

    df['station_num'] = station_list
    max_station_index = df[df.station_num == max(df['station_num'])].index[-1]
    df = df[df.index <= max_station_index]
    min_station_index = df[df.station_num == min(df['station_num'])].index[0]
    df = df[df.index >= min_station_index]

    df_tmp = df
    df = df.groupby(['station_lat', 'station_long'])['distance'].min().reset_index()
    final = pd.DataFrame(route_lat_lon)
    final.columns = ['station_lat', 'station_long']
    final = final.drop('station_long',axis=1)
    final = final.merge(df, how='inner', on=['station_lat'])
    final = final.merge(df_tmp, how='left', on=['station_lat', 'station_long'])
    final = final[final.distance_x == final.distance_y].reset_index(drop=True).drop('distance_y',axis=1)
    final.rename(columns={'distance_x':'distance'},inplace=True)
    final = final.drop_duplicates(subset=['station_num'], keep='first')
    return final


def append_all_loop(start,end,route, gps_data, path, gps_data_lat_lon, gps_imei, route_num):
    for i in range(len(start)):
        if i == 0:
            df = find_closest_distance(start = start.iloc[i], end = end.iloc[i], k=10, route_lat_lon = route, gps_data=gps_data, gps_data_lat_lon = gps_data_lat_lon)
            df['loop_num'] = [i]*len(df)
        else:
            tmp = find_closest_distance(start = start.iloc[i], end = end.iloc[i], k=10, route_lat_lon = route, gps_data=gps_data, gps_data_lat_lon = gps_data_lat_lon)
            tmp['loop_num'] = [i]*len(tmp)
            df = pd.concat([df,tmp],axis=0)

    df = df.reset_index(drop=True)
    df['path'] = [path]*len(df)
    df['route_num'] = [route_num]*len(df)
    df['gps_imei'] = [gps_imei]*len(df)
    return df.sort_values('time')