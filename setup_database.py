""" sets up a sqlite database to store data """

# -- libs

import sqlite3
import os

# -- create the table to hold the data

# location of database file
DB_FILE = os.path.join(os.getcwd(), "particledata.db")

# table def
particles = """
CREATE TABLE 'particles' (
'key' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
'time' TEXT,
'pm25' NUMERIC,
'pm10' NUMERIC,
'poll_id' TEXT)
"""

aggregated = """
CREATE TABLE 'aggregated' (
'key' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
'time' TEXT,
'pm25_min' NUMERIC,
'pm25_max' NUMERIC,
'pm25_mean' NUMERIC,
'pm25_std' NUMERIC,
'pm10_min' NUMERIC,
'pm10_max' NUMERIC,
'pm10_mean' NUMERIC,
'pm10_std' NUMERIC,
'poll_id' TEXT)
"""

# create and commit the table
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()
cur.execute(particles)
cur.execute(aggregated)
conn.commit()
conn.close()
