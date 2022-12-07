import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid
import sys

# Spark session & context
spark = (SparkSession
    .builder
    .getOrCreate()
)

class pythonFunctions:
    @udf
    def generate_uuid():
        return str(uuid.uuid4().hex)

postgres_db = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]
concat_arrival_files = sys.argv[4]
link_folder = sys.argv[5]


d = pd.read_csv(concat_arrival_files)
routes = d.route_num.unique().tolist()
path_list = ['go','back']

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
    # save files
    link.to_csv(link_folder + f'link_{route_num}.csv', index=False)


# Spark Operation
df = (spark.read.format('csv')
              .option("header", True)
             .load(link_folder + "*.csv"))

df = (df.withColumn("link_time",col("link_time").cast('integer'))
              .withColumn("routes", col("routes").cast('integer'))
              .withColumn("gps_imei", col("gps_imei").cast(LongType()))
             ).select("link_name","link_time","link_timestamp","path","gps_imei","routes")

df = df.withColumn("uuid", pythonFunctions.generate_uuid())


# upload to postgres
(df.write
 .format("jdbc")
 .option("url", postgres_db)
 .option("dbtable", "public.link_time")
 .option("user", postgres_user)
 .option("password", postgres_pwd)
 .mode("overwrite") # to append change to mode("append")
 .save())