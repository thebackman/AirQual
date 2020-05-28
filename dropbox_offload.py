""" send a day of air qual data to dropbox """


## CRON ########################################################################


# sudo crontab -e
# 30 23 * * * python3 /home/pi/Projects/AirStuff/daily_dropbox_offload.py
# in english, at 23:30 every day (when all sensor values are taken)


## SETUP #######################################################################


# -- libs

from datetime import datetime
import os
import sqlite3
import dropbox
import json
import pandas as pd


# -- paths

PROJ_FOLDER = "/home/pi/Projects/AirStuff"
CREDSPATH = os.path.join(PROJ_FOLDER, "creds.json")
DB_FILE = os.path.join(PROJ_FOLDER, "particledata.db")
CONN = sqlite3.connect(DB_FILE)


## FUNCTIONS ###################################################################


def upload_file(accesstoken, localfilepath, remotefilename):
    """ uploads a file to dropbox """
    # initiate client
    client = dropbox.Dropbox(accesstoken)
    # upload file
    client.files_upload(open(localfilepath, "rb").read(), remotefilename)


def get_data_to_file(first_date, last_date, conn, proj_folder):
    """ get xxx days of data from the db, store as csv """
    
    # query
    query = f"""
    with inbetween as (
    select *, date(time) as datecol
    from aggregated)
    select * from inbetween
    where datecol between '{first_date}' and '{last_date}'"""
    
    # get the data 
    df_agg = pd.read_sql_query(query, con = conn)
    
    # save it
    filepath = os.path.join(proj_folder, "aggregated.csv")
    df_agg.to_csv(filepath)
    
    # return the filepath
    return filepath


## RUN #########################################################################

if __name__ == "__main__":
    
    # -- pid
    
    pid = os.getpid()
    
    # -- today
    
    today = datetime.today().strftime('%Y-%m-%d')
    
    # -- get creds
    
    with open(CREDSPATH, 'r') as f:
        creds = json.load(f)
        
    # -- get data
        
    data_file = get_data_to_file(
            first_date = today,
            last_date = today,
            conn = CONN,
            proj_folder = PROJ_FOLDER)    
    
    # -- upload it
    
    upload_file(
            accesstoken = creds["token"],
            localfilepath = data_file,
            remotefilename = f"/Airstuff/aggr_{today}_{pid}.csv",)
        
    # -- delete it
    os.remove(data_file)
