#######################################################
#######################################################
########### Trinity Weather Database Upload ###########
## School: Georgia Tech
## Created: Spring 2023
## Author: Sofia Stepanoff
## Email: Sofiastepanoff@gatech.edu
## Contributors: Matthew Potts
## PI: Neopmuk Otte
## System: Ubuntu
########################################################

########################################################
################# Code Description #####################
## This code uploades weather database files that come in
## from the weather station located at the Trinity Telescope
## site. The data comes in at 60 lines a minute. This code has two
## processes: 1) continous uploading 2) backlog uploading
## In process 1. when the code starts the system will take the most recently
## updated file in the and check the database to compare what has
## been uploaded and what has not been uploaded. The code will then upload all
## the data in that days file that was not uploaded and then the process will continue
## every 56 seconds the code checks the database for the last time. Loades in the file
## and then does the compare. This after the first run through will produce around 60 lines to
## upload every 60 seconds. The file depending on size takes anywhere from 10-12 seconds to upload.
# In process 2. the system will check the file last line to the last line in the databsae. If it
## does not exist then the file will be uploaded. If it does exist then it will be checked if all
## or some has been uploaded. The non uploaded portion is uploaded. (Here some files have different time convetions
## causeing it to not read the time into a datetime object this needs to be fixed).
## the upload is done through influxdb. This is downloaded onto the computer via terminal.
##########################################################

##########################################################
############ Installation Instructions ###################
# For installation make sure influxdb is installed in the terminal
# you can check this by running "influx"
# packages required: InfluxDBClient, Pandas, numpy, glob,os, datetime, multiprocess
# change the variable "wd_path" to update to the new weather data location
# change the database variables "username" --> user name to computer
# "password" --> password to computer, "database" --> name of influxdb database
# "port" --> the port being used by influxdb (should show with influx)
# "host" --> should just be "localhost"
##########################################################

##########################################################
from influxdb import InfluxDBClient
import pandas as pd
import numpy as np
import glob
import os
import time
from datetime import datetime
import multiprocessing




# gets all the files from the weather database folder
# Takes: a folder_path
# Returns: array of files paths
def get_filenames(folder_path):
    arr_filenames = glob.glob(wd_path + '//*')
    #print(arr_filenames)
    return arr_filenames

# gets the last file from the array of files
# Takes: a array of filenames
# Returns: gets the last file path in the list of file paths
def get_last_file(arr_filenames):
    last_file = max(arr_filenames)
    #print(last_file)
    return last_file

# converts filename to measurement name for database
# Takes: a single file path
# returns just the 8 digit YYYYMMDD
def cov_filename_ment(file):
    ment = file[-8:]
    #print(ment)
    return ment

# check 
#print(cov_filename_ment(get_last_file(get_filenames(wd_path))))

# convert time to datatime object for continuous upload
# takes: a pandas dataframe
# check if this is even used
def time_epoch_ms(df):
    df['DateTime'] = pd.to_datetime(df['DateTime'], format='%Y-%m-%dT%H:%M:%S.%f')
    #df['DateTime'] = df['DateTime'].astype('int64')
    #print(df['DateTime'])


# Function to read
# #last N lines of the file
# takes: a file path and the number of lines from the end of the file needed
# this is used to gets the last line of files for double checking old files
# returns: last lines
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
 

# get the last line of the file from the weather data folder
# Takes: a single file
# Returns: the last line of the file
def get_llines_file(file):
    #print(file)
    last_lines = LastNlines(file, 2) # I found that 2 works for consistantly getting the last line
    #print('last_line',last_line)
    
    return last_lines

# Read the file into a pandas DataFrame
# Takes: a single file
# Returns: Dataframe
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
    time_epoch_ms(df) # convert to datetime object for the datetime column
    return df

# Create a list/dictionary of data points from the DataFrame this is needed to uplaod to influxdb
# Takes a single file file
# list of datapoints
def cre_df_list(file):
        df = mak_df(file)
        ment = cov_filename_ment(file)
        data_points = [] # makes the data_points

        for index, row in df.iterrows(): # goes through each line of the datafram and makes it a measurement to upload to influxdb
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

# queries a measurement for the last time in the last uploaded line
# Takes: a measurement
# Returns: last time in the database or 0
def query_ment_ltimedb(ment):
    #query = f'SELECT last(*) FROM \"{str(ment)}\"'

    # Query the last datapoint in the measurement
    try:
        result = client.query(f'SELECT * FROM "{ment}" ORDER BY time DESC LIMIT 1') # query the database for a measurement
        #   print(result)
        count = result.get_points().__next__().get('time') # gets just the time out of all the columns in the last line

        # Definity needs to be edited to work better for
        try:
            ltimedb = datetime.strptime(count, '%Y-%m-%dT%H:%M:%S.%fZ') # converts to a datetime object
        except:
            pass

        try:
            ltimedb = datetime.strptime(count, '%Y-%m-%dT%H:%M:%S.%f') # converts to a datetime object
        except:
            pass
        # 2022-11-18T16:17:07.0

        #print('ltimedb',ltimedb)
        return ltimedb

    except: # this is when there is no measurement in the database so it returns 0
        return 0

# datapoints(from a file) for a measurement last time
# Take:  the datapaoints
# Returns: last time datapoints
def dps_ment_ltimedps(dps):
    #print(dps)
    arr_ldp = list(dps)[-1] # I think this is not nessasary

    ltimedps = max(dps, key=lambda x: x['time'])['time'] # gets the last time of the database
    #print('ltimedps',ltimedps)
    return ltimedps

# gets the last line , last time from the file
# takes: file paath
# Returns: datatime object time.
def lline_ltime_file(file):
    ltime_file = str(get_llines_file(file)[0])[167:188] # converts the line to a string and then gets just the time section
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


# checks the backlog dates to see if they have been uploaded by checking the last time.
# This helps to save time in the second process
# takes: file path and measuremt
# return: 1 - if the last lines were equal or 0 - the whole file needs to be checked
def quick_check_files(file,ment):
    ltimedb = query_ment_ltimedb(ment)
    
    # last line of file
    ltime_file = lline_ltime_file(file)
    print('file', ltime_file) # shows the times
    print('db', ltimedb)
    
    if ltimedb == ltime_file:#the last line time:
        # if the last times are the same then dont load whole thing
        #print('1')
        return 1
    else:
        #print('0')
        # signal to upload whole file 
        return 0 

# gets all the last lines after the last time in the database
# Takes: datapoints and measurement
# return: the whole set of datapoints, and empty array, the subset of datapoints that need to be uploaded
def get_lines_after_ltimedb(dps,ment):
    ltimedb = query_ment_ltimedb(ment)

    ltimedps = dps_ment_ltimedps(dps)

    if ltimedb == 0:
        return dps # if there is no time then the whole set of datapoints is sent to upload

    elif ltimedps == ltimedb: # nothing new to uplaod
        print('No new data to upload')
        return []

    elif ltimedps > ltimedb: # needs to get all the extra data points no in the database and prepare the array
        print('Batch is being generated')
        print(ltimedb)
        ltimedps=ltimedps.to_pydatetime() # makes it a datetime.object
        print(ltimedps) 


        #dps1 = list(dps)
        # goes through all the datapoints and gets just the times and gets the datapoints that need to be uploaded
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
        print('Error? in get_lines_after_ltimedb ')

#query_ment_ltime(cov_filename_ment(get_last_file(get_filenames(wd_path))))
# check all the files script to run through the second process
# this runs for a for loop of all the measurments and checks if they have been uploaded
# takes: nothing
# reuturns: nothing
def check_all_files():
    # get list of files
    arr_files = get_filenames(wd_path)
    #print(len(arr_files))
    # for loop through the files
    err_upload = [] # creates an error arrary to print
    for i in range(len(arr_files)):
        t0 = time.time()
        ment = cov_filename_ment(arr_files[i]) # gets the measurent
        
        print("A--ment", ment) # shows this is process is 2
        #print(arr_files[i])
        # when the quick_check_files returned 1 it was already uploaded
        if quick_check_files(arr_files[i],ment) == 1:
            print(f'A--ment: {ment} uploaded already')
        # quick check if last line = last db
        # if it is 0 then the whole file needs to be uploaded
        else:
            print(f'A--ment: {ment} uploading')
            try:
                data_points = cre_df_list(arr_files[i]) # gets the datapoints
            
                upload_points=get_lines_after_ltimedb(data_points,ment) # gets the lines that need to be uploaded

                print(f'A--Time to load whole file {time.time()-t0}')

                # upload points in batches in 5000
                t0 = time.time()
                batch_size = 5000
                try:
                    for i in range(0, len(upload_points), batch_size):

                        batch = upload_points[i:i+batch_size]
                        #print(f'batch size {len(batch)}')
                        
                        client.write_points(points=batch) # writes points to the database
                    print(f'A--Time to complete {ment}: {time.time()-t0}')
                        
                except:

                    err_upload.append(f'{ment}:{i}-{i+batch_size}') # updates the error for printing later
                    print(f'A--something failed upload {ment}')
                    i =+1 # updated to the next i
            
            except:
                err_upload.append(f'{ment}: creating data points failed')
                print(f'A--something failed loading data {ment}')
                i =+1     



            
    print('errors [ment,loc]',err_upload)
    return print('A--Copmleted backlog data upload')

# this will be process one
# write the lines every 60 seconds of the last file last 60 lines with time check for no overlap
# when the code begins it checks the whole files and uploades any points not in the database
def continous_upload():
    err = 0 # sets the errors

    while err < 5: # as long as an error has not happened  5 times in a row the code continues to run (aka maybe no innternet connection)
        t0 = time.time()
        data_points = cre_df_list(get_last_file(get_filenames(wd_path)))
        ment = cov_filename_ment(get_last_file(get_filenames(wd_path)))
        print("C--ment", ment) # second processes
        upload_points=get_lines_after_ltimedb(data_points,ment) # points that need to be uploaded
        print(f'C--Time to load whole file {time.time()-t0}')
        try:
            if len(upload_points) ==0: # if ther points =0 wait a little bit
                time.sleep(56)

            else: 
                t0 = time.time()
                batch_size = 5000 # uploads in a max of 5000 lines
                for i in range(0, len(upload_points), batch_size):
        
                    batch = upload_points[i:i+batch_size] # created the points to upload
                    print(f'C--batch size {len(batch)}')
        
                    client.write_points(points=batch) # uploads to database
                    print(f'C--Time to upload {time.time()-t0}')
                time.sleep(56) # wait a bit
                    
            err = 0 # resets the error
            
        except Exception as e:
            
            print(f'C--Error Uploading: {e} at time {time.now()}')
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

    ################ Runs the two processes ###################
    p1=multiprocessing.Process(target=check_all_files)
    p2= multiprocessing.Process(target=continous_upload)

    # start both processes
    p1.start()
    p2.start()

    # wait for both processes to finish
    p1.join()
    p2.join()
