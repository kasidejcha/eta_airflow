from glob import glob
import pandas as pd

files = glob('vehicles/*.xlsx')

tmp = """6-21
6-22
6-23
6-24
6-25
6-26
6-27
6-28
6-29
6-30
6-31
6-32
6-33
6-35
6-34
6-36
6-37
6-38
6-39"""
tmp = tmp.split('\n')


# Merge and Clean
for i in range(len(files)):
    if i == 0 :
        df_v = pd.read_excel(files[i],header=None, skiprows=1)
        df_v = df_v.drop(0,axis=1)
        df_v.columns = ['num', 'routes','chasis', 'gps']
    else:
        if files[i] == 'vehicles/6.xlsx':
            new_df_v = pd.read_excel('vehicles/6.xlsx', header=None)
            new_df_v.columns = ['num', 'routes','chasis', 'gps']
            new_df_v['routes'] = tmp
            df_v = pd.concat([df_v, new_df_v],axis=0,)
        else:
            new_df_v = pd.read_excel(files[i],header=None, skiprows=1)
            new_df_v = new_df_v.drop(0,axis=1)
            new_df_v.columns = ['num', 'routes','chasis', 'gps']
            df_v = pd.concat([df_v, new_df_v],axis=0)

df_v.reset_index(drop=True,inplace=True)
routes=[]
for i in range(len(df_v)):
    routes.append(df_v.routes[i].split('-')[0])
df_v['routes'] = routes


# save files
df_v.to_csv('vehicles/vehicles.csv', index=False)