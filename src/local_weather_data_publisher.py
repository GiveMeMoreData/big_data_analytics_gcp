import logging
import os
from io import BytesIO
from time import sleep

import pandas as pd
import requests
from google.cloud import pubsub_v1, storage
import configparser

config = configparser.ConfigParser()
config.read('../credentials/project_details.ini')
project_id = config['GCP']['project_id']
topic_id = config['GCP']['weather_topic_id']

APP_ID = config['OPENWEATHERMAPS']['app_id']
JOB_INTERVAL = 300  # in seconds


def get_weather_data(lat, lon, api_path="https://api.openweathermap.org/data/2.5/weather"):
    response = requests.get(f"{api_path}?lat={lat}&lon={lon}&appid={APP_ID}")
    data = response.json()
    if data['cod'] != 200:  # I don't know why its 'cod' not 'code'
        raise ValueError("Openweathermap returned no data")
    return data


def load_location_data():
    def download_blob(bucket_name="locations_europe",
                      source_blob_name="geonames-all-cities-with-a-population-1000.csv"):
        """Downloads a blob from the bucket."""
        logging.info("Downloading locations")
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        contents = blob.download_as_string()
        logging.info("Locations downloaded")
        return contents

    byte_data = download_blob()
    locations = pd.read_csv(BytesIO(byte_data), sep=';')
    locations = locations[['Geoname ID', 'Name', 'ASCII Name', 'Country name EN', 'Coordinates']]
    locations = pd.concat([
        locations,
        locations['Coordinates'].str.split(',', expand=True).rename(columns={0: 'lat', 1: 'lon'})
    ], axis=1)
    locations = locations.drop('Coordinates', axis=1)
    return locations


def get_country_cities(locations, country_en_name='Belgium'):
    country_cities = locations.loc[locations['Country name EN'] == country_en_name]
    if country_cities.empty:
        raise ValueError("Invalid country name ", country_en_name)
    return country_cities


def send_data_to_topic(str_data):
    publisher.publish(topic_path, str_data.encode("utf-8"))


def collect_weather_for_cities_in_dataframe(cities_df: pd.DataFrame):
    logging.info(f"Data collection process for {cities_df.shape[0]} started")
    for _, row in country_cities.iterrows():
        try:
            weather_data_raw = get_weather_data(lat=row['lat'], lon=row['lon'])
            send_data_to_topic(str_data=str(weather_data_raw))
        except ValueError as e:
            print(e)

    logging.info(f"Data collection process completed")


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../credentials/project.json"

    logging.info("Creating publisher")
    publisher = pubsub_v1.PublisherClient()
    logging.info("Publisher created")
    topic_path = publisher.topic_path(project_id, topic_id)

    # load locations_info
    locations = load_location_data()
    country_cities = get_country_cities(locations)

    logging.info("Starting job loop")
    while True:
        collect_weather_for_cities_in_dataframe(country_cities)
        sleep(JOB_INTERVAL)

