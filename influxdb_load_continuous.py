from influxdb import InfluxDBClient
import pandas as pd
import numpy as np
import glob
import os
import time
from datetime import datetime
import multiprocessing



# gets the files from the weather database folder
def get_filenames(file_path):
    arr_filenames = glob.glob(wd_path + '//*')
    #print(arr_filenames)
    return arr_filenames

# gets the last file from the array of files
def get_last_file(arr_filenames):
    last_file = max(arr_filenames)
    #print(last_file)
    return last_file

# converts filename to measurement name for database
def cov_filename_ment(file):
    ment = file[-8:]
    #print(ment)
    return ment

# check 
#print(cov_filename_ment(get_last_file(get_filenames(wd_path))))

def time_epoch_ms(df):
    df['DateTime'] = pd.to_datetime(df['DateTime'], format='%Y-%m-%dT%H:%M:%S.%f')
    #df['DateTime'] = df['DateTime'].astype('int64')
    #print(df['DateTime'])


# Function to read
# last N lines of the file
def LastNlines(fname, N):
     
    # assert statement check
    # a condition
    assert N >= 0
     
    # declaring variable
    # to implement
    # exponential search
    pos = N + 1
     
    # list to store
    # last N lines
    lines = []
     
    # opening file using with() method
    # so that file get closed
    # after completing work
    with open(fname) as f:
         
        # loop which runs
        # until size of list
        # becomes equal to N
        while len(lines) <= N:
             
            # try block
            try:
                # moving cursor from
                # left side to
                # pos line from end
                f.seek(-pos, 2)
         
            # exception block
            # to handle any run
            # time error
            except IOError:
                f.seek(0)
                break
             
            # finally block
            # to add lines
            # to list after
            # each iteration
            finally:
                lines = list(f)
             
            # increasing value
            # of variable
            # exponentially
            pos *= 2
             
    # returning the
    # whole list
    # which stores last
    # N lines
    return lines[-N:]
 


def get_llines_file(file):
    #print(file)
    last_lines = LastNlines(file, 2)
    #print('last_line',last_line)
    
    return last_lines

# Read the file into a pandas DataFrame
def mak_df(file):

    
    header = [
        "Node", "RelativeWindDirection", "RelativeWindSpeed", "CorrectedWindDirection",
        "AverageRelativeWindDirection",
        "AverageRelativeWindSpeed", "RelativeGustDirection", "RelativeGustSpeed",
        "AverageCorrectedWindDirection",
        "WindSensorStatus", "Pressure", "Pressure_at_Sea_level", "Pressure_at_Station", 
        "Relative_Humidity","Temperature", "Dewpoint",
        "Absolute_Humidity", "compassHeading", "WindChill", "HeatIndex", "AirDensity", 
        "WetBulbTempature", "SunRiseTime", "SolarNoonTime", "SunsetTime",
        "Position of the Sun", "Twilight(Civil)","Twilight(Nautical)",
        "Twilight(Astronomical)", "X_Tilt", "Y_Tilt", "Z_Orientation", "User_Information_Field",
        "DateTime","Supply_Voltage", "Status", "Checksum"]
    df = pd.read_csv(file, names=header)
    time_epoch_ms(df)
    return df

# Create a list of data points from the DataFrame
def cre_df_list(file):
        df = mak_df(file)
        ment = cov_filename_ment(file)
        data_points = []

        for index, row in df.iterrows():
            data_point = {
                'measurement': ment,
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
                    #'WindChill': row['WindChill'], # these lines have no data and it messes everything up
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

        return data_points

# check intial systsems 
#cre_df_list(get_last_file(get_filenames(wd_path)))
#print('Intializeation done')
# this will be one process

# queries a measuremtn for the last time
def query_ment_ltimedb(ment):
    #query = f'SELECT last(*) FROM \"{str(ment)}\"'

    # Query the last datapoint in the measurement
    try:
        result = client.query(f'SELECT * FROM "{ment}" ORDER BY time DESC LIMIT 1')
        #   print(result)
        count = result.get_points().__next__().get('time')
        try:
            ltimedb = datetime.strptime(count, '%Y-%m-%dT%H:%M:%S.%fZ')
        except:
            pass

        try:
            ltimedb = datetime.strptime(count, '%Y-%m-%dT%H:%M:%S.%f')
        except:
            pass
        # 2022-11-18T16:17:07.0

        #print('ltimedb',ltimedb)
        return ltimedb

    except:
        return 0

def dps_ment_ltimedps(dps):
    #print(dps)
    arr_ldp = list(dps)[-1]
    

    ltimedps = max(dps, key=lambda x: x['time'])['time']
    #print('ltimedps',ltimedps)
    return ltimedps

def lline_ltime_file(file):
    ltime_file = str(get_llines_file(file)[0])[167:188]
    try:
        ltime_file = datetime.strptime(ltime_file, '%Y-%m-%dT%H:%M:%S.%fZ')
    except:
        pass

    try:
        ltime_file = datetime.strptime(ltime_file, '%Y-%m-%dT%H:%M:%S.%f')
    except:
        pass
    #print('hey', ltime_file)
    return ltime_file



def quick_check_files(file,ment):
    ltimedb = query_ment_ltimedb(ment)
    
    # last line of file
    ltime_file = lline_ltime_file(file)
    print('file', ltime_file)
    print('db', ltimedb)
    
    if ltimedb == ltime_file:#the last line time:
        # if the last times are the same then dont load whole thing
        #print('1')
        return 1
    else:
        #print('0')
        # signal to upload whole file 
        return 0 

def get_lines_after_ltimedb(dps,ment):
    ltimedb = query_ment_ltimedb(ment)

    ltimedps = dps_ment_ltimedps(dps)

    if ltimedb == 0:
        return dps # if there is no time then the whole set of datapoints is sent to upload

    elif ltimedps == ltimedb:
        print('No new data to upload')
        return []

    elif ltimedps > ltimedb:
        print('Batch is being generated')
        print(ltimedb)
        ltimedps=ltimedps.to_pydatetime() 
        print(ltimedps) 


        #dps1 = list(dps)
        for i in range(len(dps)):
            times = str(dps[i])[77:103]
            if times == str(ltimedb):
                #print(times)
                #print(f'row matching is {i}')
                r_upload = dps[i:] 
                #print(r_upload)
                return r_upload

            
        
        #print(location)
        # get all the lines not in dp
        
    else:
        print('Error?')

#query_ment_ltime(cov_filename_ment(get_last_file(get_filenames(wd_path))))

def check_all_files():
    # get list of files
    arr_files = get_filenames(wd_path)
    #print(len(arr_files))
    # for loop through the files
    err_upload = []
    for i in range(len(arr_files)):
        t0 = time.time()
        ment = cov_filename_ment(arr_files[i])
        
        print("A--ment", ment)
        #print(arr_files[i])
        if quick_check_files(arr_files[i],ment) == 1:
            print(f'A--ment: {ment} uploaded already')
        # quick check if last line = last db

        else:
            print(f'A--ment: {ment} uploading')
            try:
                data_points = cre_df_list(arr_files[i])
            
                upload_points=get_lines_after_ltimedb(data_points,ment)

                print(f'A--Time to load whole file {time.time()-t0}')

                # upload points
                t0 = time.time()
                batch_size = 5000
                try:
                    for i in range(0, len(upload_points), batch_size):

                        batch = upload_points[i:i+batch_size]
                        #print(f'batch size {len(batch)}')
                        
                        client.write_points(points=batch)
                    print(f'A--Time to complete {ment}: {time.time()-t0}')
                        
                except:

                    err_upload.append(f'{ment}:{i}-{i+batch_size}')
                    print(f'A--something failed upload {ment}')
                    i =+1
            
            except:
                err_upload.append(f'{ment}: creating data points failed')
                print(f'A--something failed loading data {ment}')
                i =+1     



            
    print('errors [ment,loc]',err_upload)
    return print('A--Copmleted backlog data upload')


# To Do: 

# itnial process run once at start up check all files and upload all missed data points
# check / write all the lines prior to the 60 lines if they were not writen
# check if the database has a measurement
# get all measurements
# get all file names to measurement names, compare list
# all files in both get time checked 
 




## Daily uploading (Done without errors and clean up)

## if there is a measurement check to make sure the last time in the databases matches the last time in the file
## get last time from database in that measurement
## get last time from file and convert to nanosecods
## if the times are equal - Done
## if the time in file is greater then databsae use the last time in the database and find the next line in the file and upload from there
## else - print error


# all files not in both instant upload






# this will be one process
# write the lines every 60 seconds of the last file last 60 lines with time check for no overlap
def continous_upload():
    err = 0

    while err < 5:
        t0 = time.time()
        data_points = cre_df_list(get_last_file(get_filenames(wd_path)))
        ment = cov_filename_ment(get_last_file(get_filenames(wd_path)))
        print("C--ment", ment)
        upload_points=get_lines_after_ltimedb(data_points,ment)
        print(f'C--Time to load whole file {time.time()-t0}')
        try:
            if len(upload_points) ==0:
                time.sleep(56)

            else: 
                t0 = time.time()
                batch_size = 5000
                for i in range(0, len(upload_points), batch_size):
        
                    batch = upload_points[i:i+batch_size]
                    print(f'C--batch size {len(batch)}')
        
                    client.write_points(points=batch)
                    print(f'C--Time to upload {time.time()-t0}')
                time.sleep(56)
                    
            err = 0
            
        except Exception as e:
            
            print(f'C--Error Uploading: {e} at time FIX THIS')
            # maybe make this a file in the future
            err =+ 1 # updates the error occured if 5 happen then the program will stop
            time.sleep(60) # the system waits a minute to continue
 



if __name__ == '__main__':

    ################# Location of weather data ################

    wd_path = '/home/mpotts32/weather/'

    ###########################################################


    ################# Database intinitation ###################

    # Local host lines for access
    host = 'localhost'
    port = 8086
    username = 'mpotts32'
    password = 'Ttys@210'
    database = 'Trinity'

    # Initialize the InfluxDB client and write the points in batches
    client = InfluxDBClient(host = host, port=port, username=username, password=password)

    # Create a new database if it does not already exist
    # client.create_database(database)

    # Switch to the newly created database
    client.switch_database(database)

    ################ End of DB Inintiation ####################

    p1=multiprocessing.Process(target=check_all_files)
    p2= multiprocessing.Process(target=continous_upload)

    # start both processes
    p1.start()
    p2.start()

    # wait for both processes to finish
    p1.join()
    p2.join()
