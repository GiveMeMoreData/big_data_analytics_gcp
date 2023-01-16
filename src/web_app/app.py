from flask import Flask, render_template, Markup
from hdfs import InsecureClient
from cassandra.cluster import Cluster
from datetime import timezone, timedelta
from matplotlib.dates import DateFormatter
import flask
import numpy as np
import json
import datetime
import matplotlib.pyplot as plt
import pandas as pd
import re

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

app = Flask(__name__)
cluster = Cluster()
session = cluster.connect()
session.row_factory = pandas_factory
session.default_fetch_size = None

@app.route("/")
def main():
    return render_template("app.html")

    
@app.route("/getPlot")
def get_plot():
    dt = datetime.datetime.now(timezone.utc) - timedelta(hours=9, minutes=30)
    
    result = session.execute(f"SELECT * FROM project.predictions where dt > '{dt}'  ALLOW FILTERING;")
    df1 = result._current_rows
    order1 = np.argsort(df1.dt)
    x1 = np.array(df1.dt)[order1]
    y1 = np.array(df1.prediction)[order1]
    
    result = session.execute(f"SELECT dt, avg_price FROM project.data5minutes where dt > '{dt}' ALLOW FILTERING;")
    df2 = result._current_rows
    order2 = np.argsort(df2.dt)
    x2 = np.array(df2.dt)[order2]
    y2 = np.array(df2.avg_price)[order2]
    
    fig, ax = plt.subplots()
    ax.plot(x1, y1, label='Predicted price')
    ax.plot(x2, y2, label='Real price')
    hh_mm = DateFormatter('%H:%M')
    ax.xaxis.set_major_formatter(hh_mm)
    ax.legend()
    file_name = 'plot.svg'
    plt.savefig(file_name)
    plt.close()
    
    with open(file_name, 'r') as svg:
        file = svg.read()
        file = re.sub(r'height="345.6pt"', '', file, 1)
        file = re.sub(r'width="460.8pt"', '', file, 1)
        return Markup(file)


@app.route("/getPrediction")
def get_prediction():
    dt = datetime.datetime.now(timezone.utc) - timedelta(minutes=7)
    result = session.execute(f"SELECT * FROM project.predictions where dt > '{dt}' limit 1000 ALLOW FILTERING;")
    df = result._current_rows
    df = df[df.dt == np.max(df.dt)]
    for index, row in df.iterrows():
        json_str = f'"dt": "{str(row["dt"])}", "prediction": {"{:.2f}".format(row.prediction)}'
        return json.loads('{' + json_str + '}')
    return json.loads('{}')


@app.route("/getBatchView")
def get_batch_view():
    dt = datetime.datetime.now(timezone.utc) - timedelta(hours=9, minutes=30)
    result = session.execute(f"SELECT * FROM project.data5minutes where dt > '{dt}' limit 2000 ALLOW FILTERING;")
    df = result._current_rows
    df = df[df.dt == np.max(df.dt)]
    for index, row in df.iterrows():
        json_str = f'"dt": "{str(row["dt"])}", "avg_temp": {"{:.2f}".format(row.avg_temp - 272.15)}, "avg_feels_like_temp": {"{:.2f}".format(row.avg_feels_like_temp - 272.15)}, "avg_humidity": {"{:.2f}".format(row.avg_humidity)}, "avg_pressure": {"{:.2f}".format(row.avg_pressure)}, "avg_visibility": {"{:.2f}".format(row.avg_visibility)}, "avg_wind_deg": {"{:.2f}".format(row.avg_wind_deg)}, "avg_wind_speed": {"{:.2f}".format(row.avg_wind_speed)}, "avg_clouds": {"{:.2f}".format(row.avg_clouds)}, "avg_price": {"{:.2f}".format(row.avg_price)}'
        return json.loads('{' + json_str + '}')
    return json.loads('{}')
