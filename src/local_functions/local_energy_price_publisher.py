import logging
import os
import time
from io import BytesIO

import numpy as np
import pandas as pd
from google.cloud import pubsub_v1, storage
import configparser
import json

config = configparser.ConfigParser()
config.read('../../credentials/project_details.ini')
project_id = config['GCP']['project_id']
topic_id = config['GCP']['energy_topic_id']
COUNTRY = 'Belgium'
PUBLISH_INTERVAL = 5  # is seconds, will result in sending new data to cloud every TIME_STEP seconds


def load_historical_data():
    def download_blob(bucket_name="energy_price_data",
                      source_blob_name="european_wholesale_electricity_price_data_hourly.csv"):
        """Downloads a blob from the bucket."""
        logging.info("Downloading historical_price_data")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        contents = blob.download_as_string()
        logging.info("historical_price_data downloaded")
        return contents
    byte_data = download_blob()
    prices = pd.read_csv(BytesIO(byte_data))
    return prices


def fill_data(prev_row, cur_row, time_step=5, original_time_step=60 * 60):
    in_between_points = original_time_step // time_step
    prices = np.linspace(prev_row['Price (EUR/MWhe)'], cur_row['Price (EUR/MWhe)'], in_between_points) + np.random.normal(0, 1, in_between_points)
    timestamps = pd.date_range(start=prev_row['Datetime (UTC)'], end=cur_row['Datetime (UTC)'], periods=721)
    return prices, timestamps, in_between_points


def send_data_to_topic(str_data):
    publisher.publish(topic_path, str_data)


def publishing_loop(row, prev_row, delay, country, iso_code, tz_diff):
    logging.info(f"Data publishing process started")
    # prepare data of the right velocity
    prices, timestamps, in_between_points = fill_data(prev_row, row, time_step=delay)

    next_time = time.time() + delay
    for i in range(in_between_points):
        data = {"Country": country,
                "ISO3 Code": iso_code,
                "Datetime (UTC)": str(timestamps[i]),
                "Datetime (Local)": str(timestamps[i] + tz_diff),
                "Price (EUR/MWhe)": prices[i]}
        time.sleep(max(0, next_time - time.time()))
        try:
            send_data_to_topic(json.dumps(data).encode('UTF-8'))
        except Exception as e:
            logging.exception("Problem while executing repetitive task. ", e)
        # skip tasks if we are behind schedule:
        next_time += (time.time() - next_time) // delay * delay + delay
    logging.info(f"Data publishing process finished")


def get_country_data(data, country_name='Belgium'):
    country_data = data.loc[data['Country'] == country_name]
    return country_data.reset_index(drop=True)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../../credentials/project.json"

    logging.info("Creating publisher")
    publisher = pubsub_v1.PublisherClient()
    logging.info("Publisher created")
    topic_path = publisher.topic_path(project_id, topic_id)

    historical_price_data = load_historical_data()
    country_price_data = get_country_data(historical_price_data, country_name=COUNTRY)

    first_row = country_price_data.iloc[0]
    prev_row = first_row
    iso_code = first_row['ISO3 Code']
    timezone_diff = pd.to_datetime(first_row['Datetime (Local)']) - pd.to_datetime(first_row['Datetime (UTC)'])
    country_price_data = country_price_data.drop(0)

    for _, row in country_price_data.iterrows():
        publishing_loop(row, prev_row, PUBLISH_INTERVAL, COUNTRY, iso_code, timezone_diff)
        prev_row = row




