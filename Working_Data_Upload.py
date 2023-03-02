import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import numpy as np

token = "lBC-xzUWPztlwoony04qfhqTBszguCebEUTI38aLbH3wIzG7_fJJzLon0Op8Ee9YNKXqCvulbaLQHzivbVrRTg=="
org = "Trinity"
url = "https://europe-west1-1.gcp.cloud2.influxdata.com"

write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org,debug=True)

bucket="Test1"

# Define the write api
write_api = write_client.write_api(write_options=SYNCHRONOUS)

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
# \mnt\c\\Users\Ginkl\OneDrive - Georgia Institute of Technology\TrintyWork
df = pd.read_csv('weather_20221111',names = header)
print(df)
df.head()
print(df['DateTime'].to_string())
# convert date time  precision to nanosecond precision
df['DateTime'] =(pd.to_datetime(df['DateTime'], format='%Y%m%dT%H:%M:%S')).astype(np.int64) / int(1e6)
print(int(df.loc[1].at[header[33]]))

#### End of pandas dataframe ###########

# dict v1
data={header[5]: [],header[6]: [],header[7]: [],header[8]: [],header[9]: [],header[10]: []}
# Make this loop throuh all headers
for i in range(len(df)):
    data[header[5]].append(df.loc[i].at[header[5]])
    # data[header[6]].append(df.loc[i].at[header[6]])
    # data[header[7]].append(df.loc[i].at[header[7]])
    # data[header[8]].append(df.loc[i].at[header[8]])
    # data[header[9]].append(df.loc[i].at[header[9]])
    # data[header[10]].append(df.loc[i].at[header[10]])
#.field(data[key]["species"], data[key]["count"])

# convert the pandas dataframe to dictionary so the data can be sent to the local host
data = df.to_dict('dict')
print(data)
for k in range(len(data[header[5]])):
    for key in data:

        print(key,k)

        point = (Point("census101").tag("location", "Portland").field(key, data[key][k]))
        write_api.write(bucket=bucket, org=org, record=point)
    #time.sleep(60) # separate points by 1 second

print("Complete. Return to the InfluxDB UI.")
