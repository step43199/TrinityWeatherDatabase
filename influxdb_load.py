from influxdb import InfluxDBClient
import pandas as pd
import numpy as np
import glob
import os
import time

# Local host lines for access
host = 'localhost'
port = 8086
username = 'mpotts32'
password = 'Ttys@210'
database = 'Trinity'

wd_path = '/home/mpotts32/weather/'

# Get the last file in the directory
last_file = max(glob.glob(wd_path + '//*'), key=os.path.getctime)
#print('file', last_file)
measurement = last_file[-8:]
print('measurement',measurement)



# Read the file into a pandas DataFrame
header = [
    "Node", "RelativeWindDirection", "RelativeWindSpeed", "CorrectedWindDirection",
    "AverageRelativeWindDirection",
    "AverageRelativeWindSpeed", "RelativeGustDirection", "RelativeGustSpeed",
    "AverageCorrectedWindDirection",
    "WindSensorStatus", "Pressure", "Pressure_at_Sea_level", "Pressure_at_Station", "Relative_Humidity",
    "Temperature",
    "Dewpoint",
    "Absolute_Humidity", "compassHeading", "WindChill", "HeatIndex", "AirDensity", "WetBulbTempature",
    "SunRiseTime", "SolarNoonTime", "SunsetTime",
    "Position of the Sun", "Twilight(Civil)",
    "Twilight(Nautical)",
    "Twilight(Astronomical)", "X_Tilt", "Y_Tilt", "Z_Orientation", "User_Information_Field",
    "DateTime",
    "Supply_Voltage", "Status", "Checksum"]
df = pd.read_csv(last_file, names=header)
df['DateTime'] = pd.to_datetime(df['DateTime'], format='%Y%m%dT%H:%M:%S').astype(np.int64) // 10**6
df = df.astype({'User_Information_Field':'string'})

# Create a list of data points from the DataFrame
data_points = []

for index, row in df.iterrows():
    data_point = {
        'measurement': measurement,
        'tags': {'location': 'Utah'},
        'time': row['DateTime'],
        'fields': {
            'Node': row['Node'],
            'RelativeWindDirection': row['RelativeWindDirection'],
            'RelativeWindSpeed': row['RelativeWindSpeed'],
            'CorrectedWindDirection': row['CorrectedWindDirection'],
            'AverageRelativeWindDirection': row['AverageRelativeWindDirection'],
            'AverageRelativeWindSpeed': row['AverageRelativeWindSpeed'],
            'RelativeGustDirection': row['RelativeGustDirection'],
            'RelativeGustSpeed': row['RelativeGustSpeed'],
            'AverageCorrectedWindDirection': row['AverageCorrectedWindDirection'],
            'WindSensorStatus': row['WindSensorStatus'],
            'Pressure': row['Pressure'],
            'Pressure_at_Sea_level': row['Pressure_at_Sea_level'],
            'Pressure_at_Station': row['Pressure_at_Station'],
            'Relative_Humidity': row['Relative_Humidity'],
            'Temperature': row['Temperature'],
            'Dewpoint': row['Dewpoint'],
            'Absolute_Humidity': row['Absolute_Humidity'],
            #'WindChill': row['WindChill'],
            #'HeatIndex': row['HeatIndex'],
            'compassHeading': row['compassHeading'],
            'AirDensity': row['AirDensity'],
            'WetBulbTempature': row['WetBulbTempature'],
            'SunRiseTime': row['SunRiseTime'],
            'SolarNoonTime': row['SolarNoonTime'],
            'SunsetTime': row['SunsetTime'],
            'Position of the Sun': row['Position of the Sun'],
            'Twilight(Civil)': row['Twilight(Civil)'],
            'Twilight(Nautical)': row['Twilight(Nautical)'],
            'Twilight(Astronomical)': row['Twilight(Astronomical)'],
            'X_Tilt': row['X_Tilt'],
            'Y_Tilt': row['Y_Tilt'],
            'Z_Orientation': row['Z_Orientation'],
            #'User_Information_Field': row['User_Information_Field'],
            'Supply_Voltage': row['Supply_Voltage'],
            'Status': row['Status'],
            'Checksum': row['Checksum']
       }
    }
    data_points.append(data_point)

# Initialize the InfluxDB client and write the points in batches
client = InfluxDBClient(host = host, port=port, username=username, password=password)

# Create a new database if it does not already exist
# client.create_database(database)

# Switch to the newly created database
client.switch_database(database)

batch_size = 4999
t0 = time.time()
for i in range(0, len(data_points), batch_size):
    
    batch = data_points[i:i+batch_size]
    
    
    client.write_points(points=batch)
    print(f'Time to upload {time.time()-t0}')
    
    
print(f"Uploaded {len(df)} points from {last_file}. Complete. Return to the InfluxDB UI.")





















'''








'Temperature': row['Temperature'],
            'Dewpoint': row['Dewpoint'],
            'Absolute_Humidity': row['Absolute_Humidity'],
            'compassHeading': row['compassHeading'],
            'WindChill': row['WindChill'],
            'HeatIndex': row['HeatIndex'],
            'AirDensity': row['AirDensity'],
            'WetBulbTempature': row['WetBulbTempature'],
            'SunRiseTime': row['SunRiseTime'],
            'SolarNoonTime': row['SolarNoonTime'],
            'SunsetTime': row['SunsetTime'],
            'Position of the Sun': row['Position of the Sun'],
            'Twilight(Civil)': row['Twilight(Civil)']




import pandas as pd
from influxdb import InfluxDBClient

# Load CSV file into a pandas dataframe

df = pd.read_csv('/home/mpotts32/weather/weather_20230313')

print(df)

# Convert the DataFrame to a dictionary
data = df.to_dict(orient='records')

# Convert the dictionary to InfluxDB line protocol
lines = []
for item in data:
    tags = ','.join([f'{k}={v}' for k, v in item.items() if k != 'DateTime'])
    fields = ','.join([f'{k}={v}' for k, v in item.items() if k != 'DateTime' and not pd.isna(v)])
    line = f'{item["measurement"]},{tags} {fields} {int(item["DateTime"])}'
    lines.append(line)

# Connect to InfluxDB
client = InfluxDBClient(host='localhost', port=8086)

# Create a new database if it does not already exist
client.create_database('my_database')

# Switch to the newly created database
client.switch_database('my_database')

# Write data to InfluxDB
client.write('\n'.join(lines),measurement_name='20230313')

print('completed points write')

# Query the database for all data
result = client.query('SELECT * FROM "measurement_name"')

# Print the query result
print(result)
'''
