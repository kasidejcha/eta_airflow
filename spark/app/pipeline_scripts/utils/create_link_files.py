import pandas as pd
from glob import glob



route_num_list = [6]

# combine all data
for route_num in route_num_list:
    back_files = glob(f'output/arrival_time/*{route_num}*')
    for i in range(len(back_files)):
        if i == 0:
            df = pd.read_csv(back_files[i])
        else:
            tmp = pd.read_csv(back_files[i])
            df = pd.concat([df,tmp], axis=0)
    df['route_num'] = [route_num]*len(df)
    df = df.sort_values('time').reset_index(drop=True)
    df.to_csv(f'output/combine_arrival_time/{route_num}.csv', index=False)
    print(df.head())
    print(df.tail())


    # combine to links
    link=[]
    gps = pd.read_csv(f'output/combine_arrival_time/{route_num}.csv')
    # gps = gps[gps.path == 'go']
    path_list = ['go', 'back']

    for path in path_list:
        for gps_imei in gps.gps_imei.unique().tolist():
            df = gps[gps.gps_imei == gps_imei]
            df = df[df.path == path]
            print(f"Current gps imei: {gps_imei}, Current path: {path}")

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
                    link = pd.concat([link,tmp],axis=0)
    link = link.sort_values('link_timestamp')
    link.columns = ['link_name', 'link_time(sec)', 'link_timestamp', 'path']
    link.to_csv(f'output/link/link_{route_num}.csv', index=False)