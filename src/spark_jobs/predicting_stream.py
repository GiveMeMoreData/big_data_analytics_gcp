# -*- coding: utf-8 -*-

import numpy as np 
import pandas as pd
import json
import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql.functions import expr
import findspark
from pyspark.sql import SparkSession, SQLContext, Window
from pandas import json_normalize
import argparse
from typing import Optional
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from cassandra.cluster import Cluster
from datetime import datetime

from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.ml.feature import VectorAssembler

findspark.init()

spark = (
    SparkSession.builder
    .appName("HDFS")
    .getOrCreate()
)
spark

def receive_messages(
    project_id: str, subscription_id: str, ML_model: LinearRegression, cass_session: int, timeout: Optional[float] = None
) -> None:
    """Receives messages from a pull subscription."""
    # [START pubsub_subscriber_async_pull]
    # [START pubsub_quickstart_subscriber]

    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        json_message = json.loads(message.data.decode("UTF-8"))
        json_message_norm = json_normalize(json_message, sep='_')
        sparkDF_message = spark.createDataFrame(json_message_norm)
        answer = ML_model.transform(vectorAssembler.transform(sparkDF_message)).select(F.col("prediction"))
        #print(datetime.fromtimestamp(json_message_norm["dt"][0]), answer.collect()[0][0])
        cass_session.execute(
        """
        INSERT INTO project.predictions (dt, prediction)
        VALUES (%s, %s)
        """,
        (str(datetime.fromtimestamp(json_message_norm["dt"][0])), answer.collect()[0][0])
        )
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.
    # [END pubsub_subscriber_async_pull]
    # [END pubsub_quickstart_subscriber]

vectorAssembler = VectorAssembler(inputCols = ['main_temp', 'main_pressure', 'main_humidity', 'visibility', 'wind_speed', 'wind_deg', 'clouds_all'], outputCol = 'features')

# cluster = Cluster()
# session = cluster.connect()

def predict_and_retrain(iterations = 1, iteration_time = 60*60):
    for i in range(iterations):
        cluster = Cluster()
        session = cluster.connect()
        print("Loading retrained model...")
        lrModel = LinearRegressionModel.load("gs://hdfs-cluster2/notebooks/jupyter/model")
        receive_messages("bd-ms4", "weather-sub", lrModel, session, iteration_time)
#     cluster.shutdown()

predict_and_retrain()