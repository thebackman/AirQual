""" queries sensor each day, stores and sends to dropbox """


## CRON ########################################################################


# sudo crontab -e
# 0 8 * * * python3 /home/pi/Projects/AirStuff/read_data.py
# in english, at eight o clock every day


## SETUP #######################################################################


# -- libs

from datetime import datetime, timedelta
import random
import time
import logging
import os
import control_functions
import sqlite3
import uuid
import numpy as np
import dropbox
import json
import pandas as pd

# -- paths

PROJ_FOLDER = "/home/pi/Projects/AirStuff"
CREDSPATH = os.path.join(PROJ_FOLDER, "creds.json")
DB_FILE = os.path.join(PROJ_FOLDER, "particledata.db")
CONN = sqlite3.connect(DB_FILE)

# -- today

HEUTE = datetime.today().strftime('%Y-%m-%d')

# -- get creds

with open(CREDSPATH, 'r') as f:
    CREDS = json.load(f)

# -- globals

HOURS_FROM_NOW = 12  # for how many hours from cron time do we poll
N_POLLS = 5  # how many polls to conduct
NOW = datetime.now()
PWD = os.getcwd()
PID = os.getpid()

# -- logging

FORMAT = '%(asctime)s %(levelname)s: %(module)s: %(funcName)s(): %(message)s'
LOGFILE = os.path.join(PROJ_FOLDER, "airlog.log")
logging.basicConfig(
        level=logging.DEBUG,
        format = FORMAT,
        handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler()])

# -- initialize sensor and put it to sleep

sensor = control_functions.SDS011("/dev/ttyUSB0", use_query_mode=True)
sensor.sleep()


## FUNCTIONS ###################################################################


def create_poll_times(now = NOW, hours_from_start = HOURS_FROM_NOW, polls = N_POLLS):
    """ return poll datetimes from minutes """

    # -- get the execution time

    execute_time = datetime.now()

    # -- construct the 5 poll times

    # we have xx hours * 60 minutes
    avaliable_minutes = hours_from_start * 60

    # iterate 5 times 
    poll_times = []
    for i in range(polls):
        # print(i)
        add = random.randrange(avaliable_minutes)
        poll_times.append(execute_time + timedelta(minutes = add))

    # sort poll times
    poll_times.sort()
    
    logging.debug("Poll times in readable format")
    for t in poll_times:
        print(str(t))
        logging.debug(str(t))
    logging.debug("-----------------------------------------------------------")
    return poll_times


def save_rawdata(dbfile, value_tuple, poid):
    """ takes a value tuple from the sensor and saves the values in sqlite """
    
    # initiate the connection to the DB
    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()

    
    # get the time
    now = datetime.now()
    
    # write into the db
    cur.execute("""
                INSERT INTO particles (time, pm25, pm10, poll_id)
                VALUES (?,?,?,?);
                """,(now, value_tuple[0], value_tuple[1], poid[0]))
    
    # commit changes and close
    conn.commit()
    conn.close()


def save_aggregated(dbfile, pm25list, pm10list, poid):
    """ saves aggregated data in sqlite"""
    
    # step one, convert arrays to numpy arrays
    pm25arr = np.array(pm25list)
    pm10arr = np.array(pm10list)
    
    # step 2 aggregate
    pm25min = pm25arr.min()
    pm25max = pm25arr.max()
    pm25mean = pm25arr.mean()
    pm25std = pm25arr.std()
    
    pm10min = pm10arr.min()
    pm10max = pm10arr.max()
    pm10mean = pm10arr.mean()
    pm10std = pm10arr.std()
    
    # step 3 save in database
    # initiate the connection to the DB
    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()

    
    # get the time
    now = datetime.now()
    
    # write into the db
    cur.execute("""
                INSERT INTO aggregated
                (time, pm25_min, pm25_max, pm25_mean, pm25_std,
                 pm10_min, pm10_max, pm10_mean, pm10_std, poll_id)
                VALUES (?,?,?,?,?,?,?,?,?,?);
                """,
                (now,
                 pm25min,
                 pm25max,
                 pm25mean,
                 pm25std,
                 pm10min,
                 pm10max,
                 pm10mean,
                 pm10std,
                 poid[0]))
    
    # commit changes and close
    conn.commit()
    conn.close()

    
def query_seconds(seconds = 60):
    """ queries the sensor for seconds seconds"""
    
    # some empty arrays to store the data
    pm25 = []
    pm10 = []
    
    logging.debug(f"time is {current_time}")
    logging.debug("waking up sensor")
    
    # wake up sensor
    sensor.sleep(sleep = False)
    
    # wait 15 seconds to make sure sensor is stable
    time.sleep(15)
    
    # create a unique identifier to use for the particlar poll
    poll_uuid = [str(uuid.uuid4())]
    
    # query for xx seconds
    t_end = time.time() + seconds
    while time.time() < t_end:
        # get data from sensor
        vals = sensor.query()
        # check that the value is OK, is a tuple, then store data
        if type(vals) is tuple:
            pm25.append(vals[0])
            pm10.append(vals[1])
            save_rawdata(dbfile = DB_FILE,
                         value_tuple = vals,
                         poid = poll_uuid)
            
    # This poll run done, put it to put it to sleep
    logging.debug("polling ended")
    
    # only save aggregate data in case there is data in the arrays
    l_pm25 = len(pm25)
    l_pm10 = len(pm10)
    
    logging.debug(f"length of pm25 list is {l_pm25}")
    logging.debug(f"length of pm10 list is {l_pm10}")
    if l_pm25 > 0 and l_pm10 > 0:
        logging.debug("saving aggregated data")
        save_aggregated(dbfile = DB_FILE,
                        pm25list = pm25,
                        pm10list = pm10,
                        poid = poll_uuid)
    else:
        logging.debug("failed to save aggregate data")
    sensor.sleep()


def upload_file(localfilepath, remotefilename, accesstoken):
    """ uploads a file to dropbox """
    # initiate client
    client = dropbox.Dropbox(accesstoken)
    # upload file
    client.files_upload(open(localfilepath, "rb").read(), remotefilename)


def send_to_dropbox(date, proj_folder, creds):
    
    # query
    QUERY = f"""
    with inbetween as (
    select *, date(time) as datecol
    from aggregated)
    select * from inbetween
    where datecol = '{date}'"""
    
    # get the data 
    df_agg = pd.read_sql_query(QUERY, con = CONN)
    
    # save it
    filepath = os.path.join(proj_folder, "aggregated.csv")
    df_agg.to_csv(filepath)
        
    # upload the file    
    upload_file(
            localfilepath = filepath,
            remotefilename = f"/Airstuff/file_{date}.csv",
            accesstoken = creds["token"])
    
    # delete it
    os.remove(filepath)
    
        
## EXECUTE #####################################################################


logging.debug("----------------------------------------------------------------")
logging.debug("Starting execution")
logging.debug(f"time is: {NOW}")
logging.debug(f"working directory is: {PWD}")
logging.debug(f"The PID is {PID}")
logging.debug("Lets check when we poll data")
logging.debug("---------------------------------------------------------------")

# -- generate poll list

polly = create_poll_times()

# -- take out the max time and add 10 minutes run time

max_time = max(polly) + timedelta(minutes = 10)
max_time
logging.debug(f"max_time is {max_time}")    

# -- run the script as long as the current time is less then the max poll time

current_time = datetime.now()

# iterate over each poll time
for next_poll_time in polly:
    
    # wait until its time to poll
    while next_poll_time > current_time:
        print(f"next poll time is {next_poll_time}, time is {current_time}")
        time.sleep(1)
        current_time = datetime.now()
        
    # wake up sensor, poll, put it to sleep again
    query_seconds(seconds = 60)
    
# -- done for today
    
# get the data from the DB, and send it to dropbox
logging.debug("sending a day of data to dropbox")
send_to_dropbox(
        date = HEUTE,
        proj_folder = PROJ_FOLDER,
        creds = CREDS)


logging.debug("---------------------------------------------------------------")    
logging.debug(f"time is {current_time}. We are done for today")

# -- release the logging

logging.shutdown()
