# -*- coding: utf-8 -*-

import numpy as np 
import pyspark.sql.functions as F
import findspark
from pyspark.sql import SparkSession, SQLContext, Window
import datetime
from hdfs import InsecureClient

from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.ml.feature import VectorAssembler

findspark.init()

spark = (
    SparkSession.builder
    .appName("HDFS")
    .getOrCreate()
)

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
    
    #conf = (SparkConf().setAppName("simple")
    #    .set("spark.shuffle.service.enabled", "false")
    #    .set("spark.dynamicAllocation.enabled", "false")
    #    .set("spark.cores.max", "1")
    #    .set("spark.executor.instances","2")
    #    .set("spark.executor.memory","200m")
    #    .set("spark.executor.cores","1"))
    
    df_weather = (
        sqlContext.read.parquet(weather_file_name)  
        .withColumn('dt', F.expr('to_timestamp(int(dt / 300 ) * 300)'))
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

train_data = data.withColumnRenamed("avg_temp","main_temp") \
    .withColumnRenamed("avg_feels_like_temp","main_feels_like") \
    .withColumnRenamed("avg_pressure","main_pressure") \
    .withColumnRenamed("avg_humidity","main_humidity") \
    .withColumnRenamed("avg_visibility","visibility") \
    .withColumnRenamed("avg_wind_speed","wind_speed") \
    .withColumnRenamed("avg_wind_deg","wind_deg") \
    .withColumnRenamed("avg_clouds","clouds_all") \
    .withColumnRenamed("avg_price","price")

vectorAssembler = VectorAssembler(inputCols = ['main_temp', 'main_pressure', 'main_humidity', 'visibility', 'wind_speed', 'wind_deg', 'clouds_all'], outputCol = 'features')
df_weather_train = vectorAssembler.transform(train_data)
df_weather_train = df_weather_train.select(['features', 'price'])

lr = LinearRegression(featuresCol='features', labelCol='price',
                      maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(df_weather_train)

# Print the coefficients and intercept for linear regression
#print("Coefficients: %s" % str(lrModel.coefficients))
#print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
#trainingSummary = lrModel.summary
#print("numIterations: %d" % trainingSummary.totalIterations)
#print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
#trainingSummary.residuals.show()
#print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
#print("r2: %f" % trainingSummary.r2)

lrModel.write().overwrite().save("gs://hdfs-cluster2/notebooks/jupyter/model")

#model2 = LinearRegressionModel.load("gs://hdfs-cluster2/notebooks/jupyter/model")

