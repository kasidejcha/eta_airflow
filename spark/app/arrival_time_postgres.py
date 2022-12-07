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
arrival_files = sys.argv[4]
concat_arrival_file = sys.argv[5]

df_arrival = (spark.read.format('csv')
              .option("header", True)
             .load(arrival_files))

df_arrival = df_arrival.orderBy(df_arrival.time.asc())

df_arrival = (df_arrival.withColumn("station_lat",col("station_lat").cast('double'))
              .withColumn("station_long", col("station_long").cast("double"))
              .withColumn("distance", col("distance").cast("double"))
              .withColumn("time", col("time").cast(StringType()))
              .withColumn("station_num", col("station_num").cast(IntegerType()))
              .withColumn("loop_num", col("loop_num").cast(IntegerType()))
              .withColumn("route_num", col("route_num").cast(IntegerType()))
              .withColumn("gps_imei", col("gps_imei").cast(LongType()))
              )

df_arrival = df_arrival.withColumn("uuid", pythonFunctions.generate_uuid())

# upload to postgres
(df_arrival.write
 .format("jdbc")
 .option("url", postgres_db)
 .option("dbtable", "public.arrival_time")
 .option("user", postgres_user)
 .option("password", postgres_pwd)
 .mode("overwrite") # to append change to mode("append") # to overwrite change to mode("overwrite")
 .save())

# save to file
df_arrival.toPandas().to_csv(concat_arrival_file, index=False)