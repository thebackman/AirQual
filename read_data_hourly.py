""" reads air quality data from the sensor hourly instead of at random intervals """


## CRON ########################################################################


# sudo crontab -e
# 0 7-22 * * * python3 /home/pi/Projects/AirStuff/read_data_hourly.py
# in english, starting at 7 and reading until 22


## SETUP #######################################################################


# -- libs

from datetime import datetime
import time
import logging
import os
import control_functions
import sqlite3
import uuid
import numpy as np
import json

# -- paths

PROJ_FOLDER = "/home/pi/Projects/AirStuff"
CREDSPATH = os.path.join(PROJ_FOLDER, "creds.json")
DB_FILE = os.path.join(PROJ_FOLDER, "particledata.db")
CONN = sqlite3.connect(DB_FILE)

# -- today

HEUTE = datetime.today().strftime('%Y-%m-%d')

# -- now

CURRENT_TIME = datetime.now()

# -- get creds

with open(CREDSPATH, 'r') as f:
    CREDS = json.load(f)

# -- logging

FORMAT = '%(asctime)s %(levelname)s: %(module)s: %(funcName)s(): %(message)s'
LOGFILE = os.path.join(PROJ_FOLDER, "hourlog.log")
logging.basicConfig(
        level=logging.DEBUG,
        format = FORMAT,
        handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler()])


## FUNCTIONS ###################################################################


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


## EXECUTE #####################################################################


if __name__ == "__main__":
    
    # -- write some log data

    PWD = os.getcwd()
    PID = os.getpid()
    
    logging.debug("----------------------------------------------------------------")
    logging.debug("Starting execution")
    logging.debug(f"Time is {CURRENT_TIME}")
    logging.debug(f"working directory is: {PWD}")
    logging.debug(f"name is {__name__}")
    logging.debug(f"The PID is {PID}")
    
    # -- initialize sensor

    sensor = control_functions.SDS011("/dev/ttyUSB0", use_query_mode=True)
    
    # -- wake up sensor, poll, put it to sleep again

    query_seconds(seconds = 60)
    
    # -- end and release the logging

    logging.debug("---------------------------------------------------------------")
    logging.shutdown()
