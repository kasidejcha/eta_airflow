from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from pyspark.sql.functions import *
from functools import reduce
import sys


# Spark session & context
spark = (SparkSession
         .builder
         .getOrCreate())

sc = spark.sparkContext
sc.setLogLevel('WARN')

file_path = sys.argv[1]
dis_path = sys.argv[2]
postgres_db = sys.argv[3]
postgres_user = sys.argv[4]
postgres_pwd = sys.argv[5]

df = (spark.read.format('csv')
              .option("header", True)
             .load(file_path))

df = df.withColumn("Receiving Time", to_timestamp(col("Receiving Time")))

on = df.filter(df['Boarding Sign'] == 'IN')
off = df.filter(df['Boarding Sign'] == 'OUT')
newColumns = ['NO', 'TRANS channel', 'TRANS Type', 'Merchant', 'Route Name',
       'Plate No', 'Device ID', 'Serial No', 'Card No', 'Card Type',
       'Amount before transaction', 'Receivable', 'Autual Amount',
       'TRANS Date', 'TRANS Time', 'Receiving Time', 'Boarding Sign',
       'Station Name', 'Station Total', 'Direction', 'Merchant order number',
       'Reason for refusal', 'Liquidation status', 'TRANS Status', 'STATUS']
newColumns = [s + '_on' for s in newColumns]
oldColumns = on.schema.names
on_renamed = reduce(lambda on, idx: on.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), on)
newColumns = ['NO', 'TRANS channel', 'TRANS Type', 'Merchant', 'Route Name',
       'Plate No', 'Device ID', 'Serial No', 'Card No', 'Card Type',
       'Amount before transaction', 'Receivable', 'Autual Amount',
       'TRANS Date', 'TRANS Time', 'Receiving Time', 'Boarding Sign',
       'Station Name', 'Station Total', 'Direction', 'Merchant order number',
       'Reason for refusal', 'Liquidation status', 'TRANS Status', 'STATUS']
newColumns = [s + '_off' for s in newColumns]
oldColumns = off.schema.names
off_renamed = reduce(lambda off, idx: off.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), off)
on = on_renamed
off = off_renamed

df_join = off.join(on, (off["Card No_off"] == on["Card No_on"]) & (off['Route Name_off']==on['Route Name_on']) & (off['Receiving Time_off']>on['Receiving Time_on']))
df_join = df_join.sort(col("Receiving Time_off"),col("Receiving Time_on").desc())
df_join = df_join.dropDuplicates(['Card No_off',"Route Name_off", 'Receiving Time_off']) # keep first


dis = (spark.read.format('csv')
              .option("header", True)
             .load(dis_path))
dis = dis.withColumnRenamed("_c0","idx")
dis = dis.withColumn("idx", dis.idx.cast('integer'))
dis = dis.withColumn("distance_km",dis.distance_km.cast('float'))

dis = dis.select('idx','station_num', 'routes','direction', 'station', 'lat', 'lon','distance_km')
newColumns = ['idx','station_num', 'routes','direction', 'station', 'lat', 'lon','distance_km']
newColumns = [s + '_off' for s in newColumns]
oldColumns = dis.schema.names
dis_off = reduce(lambda dis, idx: dis.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), dis)
newColumns = ['idx','station_num', 'routes','direction', 'station', 'lat', 'lon','distance_km']
newColumns = [s + '_on' for s in newColumns]
oldColumns = dis.schema.names
dis_on = reduce(lambda dis, idx: dis.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), dis)

df_join = df_join.join(dis_on, (df_join['Station Name_on'] == dis_on['station_on']) & (df_join['Route Name_on'] == dis_on['routes_on']))
df_join = df_join.join(dis_off, (df_join['Station Name_off'] == dis_off['station_off']) & (df_join['Route Name_off'] == dis_off['routes_off']))

t = df_join.select(['Card No_off','Route Name_off', 'Station Name_on', 'Station Name_off', 'Receiving Time_on', 'Receiving Time_off','idx_on', 'idx_off'])
t = t.withColumn('idx_diff', col('idx_off')-col('idx_on'))
t = t.where(t.idx_diff>0)

t = t.join(dis, (t['idx_on']<=dis['idx']) & (dis['idx']<=t['idx_off']))
t = t.groupBy('Card No_off', 'Route Name_off', 'Station Name_on', 'Station Name_off','Receiving Time_on', 'Receiving Time_off').sum('distance_km')

(t.write
.format("jdbc")
.option("url", postgres_db)
.option("dbtable", "public.divine_trans_spark")
.option("user", postgres_user)
.option("password", postgres_pwd)
.mode("overwrite")
.save())