# -*- coding: utf-8 -*-

import numpy as np 
import pyspark.sql.functions as F
import findspark
from pyspark.sql import SparkSession, SQLContext
import datetime
from hdfs import InsecureClient
from cassandra.cluster import Cluster

def get_hdfs_files(address = "http://localhost:9870", user='bdaa', directory="weather"):
    client = InsecureClient("http://localhost:9870", user=user)
    return client.list(directory)

def get_newest_file(files):
    dates = [datetime.datetime.strptime(s[:19], "%Y-%m-%d-%H-%M-%S") for s in files]
    index_min = np.argmax(dates)
    return files[index_min]

weather_files = get_hdfs_files(directory="weather")
energy_prices_files = get_hdfs_files(directory="energy_prices")

weather_file = get_newest_file(weather_files)
energy_prices_file = get_newest_file(energy_prices_files)

def get_joined_data(weather_file_name='/user/bdaa/weather/2022-12-17-05-12-13fc975232-44b9-4c24-a9ac-7dd026527564', energy_file_name='/user/bdaa/energy_prices/2022-12-17-05-13-179225cf8b-1f0c-498c-a32b-dbcb0d8e4b45'):
    findspark.init()
    spark = (
        SparkSession.builder
        .appName('HDFS')
        .getOrCreate()
    )
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    
    df_weather = (
        sqlContext.read.parquet(weather_file_name)  
        .withColumn('dt', F.expr('to_timestamp(int(dt / 300) * 300)'))
        .dropDuplicates()
        .drop('weather_0__icon', 'base', 'sys_type', 'sys_id', 'sys_country', 'timezone', 'id', 
              'name', 'cod', 'weather_0__main', 'weather_0__description', 'coord_lon', 'coord_lat', 
              'main_temp_min', 'main_temp_max', 'sys_sunrise', 'sys_sunset', 'weather_0__id')
    )
    
    weather = df_weather.groupBy('dt') \
        .agg(F.avg('main_temp').alias('avg_temp'),
             F.avg('main_feels_like').alias('avg_feels_like_temp'),
             F.avg('main_pressure').alias('avg_pressure'),
             F.avg('main_humidity').alias('avg_humidity'),
             F.avg('visibility').alias('avg_visibility'),
             F.avg('wind_speed').alias('avg_wind_speed'),
             F.avg('wind_deg').alias('avg_wind_deg'),
             F.avg('clouds_all').alias('avg_clouds'),
         )
    
    df_prices = (
        sqlContext.read.parquet(energy_file_name)
            .withColumn('Datetime__UTC_', F.unix_timestamp('Datetime__UTC_'))
            .withColumn('Datetime__UTC_', F.expr('to_timestamp(int((Datetime__UTC_) / 300) * 300)'))
            .drop('Country', 'ISO3_Code', 'Datetime__Local_')
            .withColumnRenamed('Datetime__UTC_', 'dt_prices')
            .withColumnRenamed('Price__EUR_MWhe_', 'price')
            .groupBy('dt_prices').agg(F.avg('price').alias('avg_price'))
    )
    
    data = weather.join(df_prices, weather.dt == df_prices.dt_prices).drop('dt_prices')
    return data

data = get_joined_data(f'/user/bdaa/weather/{weather_file}', f'/user/bdaa/energy_prices/{energy_prices_file}')

cluster = Cluster()
session = cluster.connect()

pandasDF = data.toPandas()
for index, x in pandasDF.iterrows():
        session.execute(
        """
        INSERT INTO project.data5minutes (dt, avg_clouds, avg_feels_like_temp, avg_humidity, avg_pressure, avg_price, avg_temp, avg_visibility, avg_wind_deg, avg_wind_speed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (str(x["dt"]), x["avg_clouds"], x["avg_feels_like_temp"], x["avg_humidity"], x["avg_pressure"], x["avg_price"], x["avg_temp"], x["avg_visibility"], x["avg_wind_deg"], x["avg_wind_speed"])
        )


# result = session.execute('SELECT * FROM project.data5minutes;')
# for i in result:
#     print(i)

cluster.shutdown()