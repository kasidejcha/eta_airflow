import pandas as pd
from utils.utils_fn_sit import *
import warnings
import time
import argparse
from glob import glob
import os
warnings.filterwarnings("ignore")
from datetime import datetime, timedelta


def get_parameters():
    parser = argparse.ArgumentParser(description='Arrival_preprocessing')
    parser.add_argument('--route_path', type=str, default='/usr/local/spark/resources/data/routes/routes.csv', help='path to route file')
    parser.add_argument('--vehicle_path', type=str, default='/usr/local/spark/resources/data/vehicles/vehicles_30-11-22.csv', help='path to vehicle file')
    parser.add_argument('--route_num_01', type=str, default= None, help='route number')
    parser.add_argument('--route_num_02', type=str, default= None, help='route number')
    parser.add_argument('--route_num_03', type=str, default= None, help='route number')
    args = parser.parse_args()
    return args

# init
args = get_parameters()

# inputs
routes = pd.read_csv(args.route_path)
vehicles = pd.read_csv(args.vehicle_path)

# init
route_num_list = [args.route_num_01, args.route_num_02, args.route_num_03]
route_num_list = list(filter(lambda x: x is not None, route_num_list)) # drop None route number

# Routes
# Thai Star
# ['4-1(6)', '1-5(39)', '4-8(35)', '3-15(133)', '4-44(80ก.)', '4-40(56)']
# SIT
# ['4-3(17)', '4-28(529)', '2-38(8)', '4-53(149)', '1-2E(34)', '1-41(92)', '1-37(27)', '3-53',
#  '4-55(163)', '4-27E(173)', '1-39(71)', '2-42(44)', '3-25E(552)', '1-61', '2-15(97)', '1-58(525)',
#  '3-36(4)', '4-17(88)', '4-49(170)', '1-59(526)', '3-1(2)', '1-4(39)', '4-23E(140)', '1-77(26ก.)',
#  '4-15(82)', '4-61(515)', '4-45(81)', '1-3(34)']


path_list = ['G','B']
hour_6 = 1.2
hour_133 = 2.5
hour_56 = 1.5
hour_35 = 2.5
hour_80 = 2.0

vender = 'sit'
# yesterday = datetime.now() - timedelta(days=1)
# yesterday = yesterday.strftime('%Y-%m-%d')


days = ['01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18', '19', '20', '21', '22','23','24','25']
for day in days:
    yesterday = f'2022-12-{day}'
    input_file_path = f'/usr/local/spark/resources/data/gps/{vender}_gps_{yesterday}.csv'
    gps_files = [input_file_path]
    for gps_file in gps_files:
        gps = pd.read_csv(gps_file).sort_values('time').reset_index(drop=True)
        gps.rename(columns={'gps_imei':'gps', 'route':'routes'},inplace=True)
        date = yesterday
        
        # Create output folder
        gps_file_path = f'/usr/local/spark/resources/data/arrival/{date}/arrival_time/'
        isExist = os.path.exists(gps_file_path)
        if not isExist:
            os.makedirs(gps_file_path)
            print("The new directory is created!")

        for route_num in route_num_list:
            print("route_num:", route_num)
            if route_num == '4-40(56)':
                time_hour = hour_56
            elif route_num == '4-1(6)':
                time_hour = hour_6
            elif route_num == '4-44(80ก.)':
                time_hour = hour_80
            else:
                time_hour = 2.5

            for path in path_list:
                print("path:", path)
                route_lat_lon, station_num = find_route(routes, route_num, path)
                if route_lat_lon == []:
                    print('Vehicle route and master route does not match')
                    break;

                num_cars = len(vehicles[vehicles.route == str(route_num)])
                print('number of cars:', num_cars)

                for car_no in range(num_cars):
                    start_time = time.time()
                    print("route_num:", route_num)
                    print("path:", path)
                    print(f'car_no: {car_no+1} out of {num_cars}')
                    gps_imei = vehicles[vehicles.route == str(route_num)]['gps_imei'].iloc[car_no]

                    print('vehicle gps imei:',gps_imei)
                    try:
                        gps_data_lat_lon, gps_data = gps_lat_lon_no_date_filter(gps, route_num, gps_imei)
                    except:
                        print('Error in gps_lat_lon function')
                        continue

                    print('Finding direction of each loop...')
                    start_index, end_index = find_direction(station_num, route_lat_lon, gps_data_lat_lon, gps_data)
                    if len(start_index) != 0:
                        start_end_df = filter_loop(start_index, end_index, gps_data, time_hour)

                        if len(start_end_df)>0:
                            print('Calculate closest distance to each station and saving...')
                            df = append_all_loop(start = start_end_df.start, end = start_end_df.end, route = route_lat_lon, gps_data=gps_data, path=path, \
                                gps_data_lat_lon = gps_data_lat_lon, gps_imei = gps_imei, route_num=route_num)

                            # save
                            df.to_csv(gps_file_path + f'{gps_imei}_{route_num}_{path}.csv', index=False)
                        else:
                            print(f"after filter, this vehicle doesn't follow {path} path")
                    else:
                        print(f"This vehicle doesn't follow {path} path")
                    print("--- %s minutes ---" % str((time.time() - start_time)/60))
                    print('')
                    
        #             break
        #         break
        #     break
        # break