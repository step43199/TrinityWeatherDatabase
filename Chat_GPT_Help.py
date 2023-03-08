# import influxdb_client, os, time
# from influxdb_client import InfluxDBClient, Point, WritePrecision
# from influxdb_client.client.write_api import SYNCHRONOUS
# import pandas as pd
# import numpy as np
# import glob
#
#
#
# # this is a mess read last file and upload the whole bad boi
#
#
# # Local host lines for access
# token = "lBC-xzUWPztlwoony04qfhqTBszguCebEUTI38aLbH3wIzG7_fJJzLon0Op8Ee9YNKXqCvulbaLQHzivbVrRTg=="
# org = "Trinity"
# url = "https://europe-west1-1.gcp.cloud2.influxdata.com"
# bucket="Test1"
# wd_path = '/mnt/c/Users/Ginkl/OneDrive/Desktop/weather/'
# #wd_path = 'C:/Users/Ginkl/OneDrive/Desktop/weather/' # weather data path
#
#
# def get_last_file(path):
#     list_in_files = glob.glob(path+ '//*')
#     last_file = max(list_in_files, key=os.path.getctime)
#     return last_file
#
#
# def create_pd(file):
#     header = [
#         "Node", "RelativeWindDirection", "RelativeWindSpeed", "CorrectedWindDirection",
#         "AverageRelativeWindDirection",
#         "AverageRelativeWindSpeed", "RelativeGustDirection", "RelativeGustSpeed",
#         "AverageCorrectedWindDirection",
#         "WindSensorStatus", "Pressure", "Pressure at Sea level", "Pressure at Station", "Relative Humidity",
#         "Temperature",
#         "Dewpoint",
#         "Absolute Humidity", "compassHeading", "WindChill", "HeatIndex", "AirDensity", "WetBulbTempature",
#         "SunRiseTime", "SolarNoonTime", "SunsetTime",
#         "Position of the Sun", "Twilight (Civil)",
#         "Twilight (Nautical)",
#         "Twilight (Astronomical)", "X-Tilt", "Y-Tilt", "Z-Orientation", "User Information Field",
#         "DateTime",
#         "Supply Voltage", "Status", "Checksum"]
#     df = pd.read_csv(file, names=header)
#     return df
#
#
# def convert_datetime_nano(df):
#     df['DateTime'] = (pd.to_datetime(df['DateTime'], format='%Y%m%dT%H:%M:%S')).astype(np.int64) / int(1e6)
#     return df
#
#
# def convert_datetime_nano(df):
#     df['DateTime'] = pd.to_datetime(df['DateTime'], format='%Y%m%dT%H:%M:%S').astype(np.int64) // 10**6
#     return df
#
# def upload_data(filename, data):
#     with write_client:
#         for k in range(len(data['Pressure'])):
#             point = Point(str(filename[-8:])).tag("location", "Telly")
#             for key in data:
#                 t0 = time.time()
#                 point.field(key, data[key][k])
#                 print(f'The time it took to upload {time.time()-t0}s')
#             write_api.write(bucket=bucket, org=org, record=point)
#     return print(f'Uploaded {len(data)} points from {filename}')
#
#
# last_file = get_last_file(wd_path)
# print('file',last_file)
# df = create_pd(last_file)
# df = convert_datetime_nano(df)
# data = convert_datetime_nano(df)
#
# # Initializing database
# write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org,debug=True)
# write_api = write_client.write_api(write_options=SYNCHRONOUS)
#
# upload_data(last_file, data)
#
# print("Complete. Return to the InfluxDB UI.")


import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import numpy as np
import glob
import os
import time

# Local host lines for access
token = "lBC-xzUWPztlwoony04qfhqTBszguCebEUTI38aLbH3wIzG7_fJJzLon0Op8Ee9YNKXqCvulbaLQHzivbVrRTg=="
org = "Trinity"
url = "https://europe-west1-1.gcp.cloud2.influxdata.com"
bucket = "Test1"
wd_path = '/mnt/c/Users/Ginkl/OneDrive/Desktop/weather/'

# Get the last file in the directory
last_file = max(glob.glob(wd_path + '//*'), key=os.path.getctime)
print('file', last_file)

# Read the file into a pandas DataFrame
header = [
    "Node", "RelativeWindDirection", "RelativeWindSpeed", "CorrectedWindDirection",
    "AverageRelativeWindDirection",
    "AverageRelativeWindSpeed", "RelativeGustDirection", "RelativeGustSpeed",
    "AverageCorrectedWindDirection",
    "WindSensorStatus", "Pressure", "Pressure at Sea level", "Pressure at Station", "Relative Humidity",
    "Temperature",
    "Dewpoint",
    "Absolute Humidity", "compassHeading", "WindChill", "HeatIndex", "AirDensity", "WetBulbTempature",
    "SunRiseTime", "SolarNoonTime", "SunsetTime",
    "Position of the Sun", "Twilight (Civil)",
    "Twilight (Nautical)",
    "Twilight (Astronomical)", "X-Tilt", "Y-Tilt", "Z-Orientation", "User Information Field",
    "DateTime",
    "Supply Voltage", "Status", "Checksum"]
df = pd.read_csv(last_file, names=header)
df['DateTime'] = pd.to_datetime(df['DateTime'], format='%Y%m%dT%H:%M:%S').astype(np.int64) // 10**6

# Create a list of points from the DataFrame
points = []
for index, row in df.iterrows():
    point = Point(str(last_file[-8:])).tag("location", "Telly")
    for key in row.keys():
        point.field(key, row[key])
    points.append(point)

print('completed')
# Initialize the InfluxDB client and write the points in batches
write_client = InfluxDBClient(url=url, token=token, org=org)
write_api = write_client.write_api(write_options=SYNCHRONOUS)
batch_size = 2
for i in range(0, len(points), batch_size):
    t0 = time.time()
    write_api.write(bucket=bucket, org=org, record=points[i:i+batch_size])
    print(f'Time to upload {time.time()-t0}')


print(f"Uploaded {len(df)} points from {last_file}. Complete. Return to the InfluxDB UI.")

