from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json
import numpy as np
import pandas as pd



app = Flask(__name__) 


@app.route('/')
@app.route('/homepage')
def home():
    return 'Hello World'


@app.route('/stations/') #default method GET
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>') #dynamic route
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/add', methods=['POST']) 
def route_add_trip():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()


@app.route('/trips/<id>')
def route_trips_id(id):
    conn = make_connection()
    trip = get_trip_id(id, conn)
    return trip.to_json()

@app.route('/trips/average_duration/<bikeid>')
def route_average_duration(bikeid):
    conn = make_connection()
    avg_duration = average_bike_duration(bikeid,conn)
    return avg_duration.to_json()

@app.route('/trips/top5_start_station/') 
def route_top5_start_station():
    conn = make_connection()
    top5 = top5_start_station(conn)
    return top5.to_json()

@app.route('/trips/top5busiestroute') 
def route_top5busiestroute():
    conn = make_connection()
    busiestroute = top5route(conn)
    return busiestroute.to_json()


@app.route('/json', methods = ['POST']) 
def json_example():

    req = request.get_json(force=True) # Parse the incoming json data as Dictionary

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')


#post End Point 
@app.route('/trips/total_bikers', methods = ['POST'])
def route_total_bikers():
    data_input = request.get_json(force=True)
    conn = make_connection()
    return total_bikers (conn=conn,**data_input).to_json()

########################################## Functions ############################################

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 


def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query,conn)
    return result

def get_trip_id(id, conn):
    query = f"""SELECT * FROM trips WHERE id ={id}"""
    result = pd.read_sql_query(query,conn)
    return result

def average_bike_duration(bikeid,conn):
    query = f""" 
        SELECT bikeid,avgtrip
        FROM (
        SELECT  bikeid ,avg(duration_minutes) as avgtrip
        FROM trips
        GROUP BY bikeid
        )
        WHERE bikeid = {bikeid}
        
        """
    result = pd.read_sql_query(query,conn)
    return result

def top5_start_station(conn):
    query = f"""
    SELECT 
        trips.start_station_id,
        trips.start_station_name,
        COUNT(id)AS total_trips 
    FROM trips
    GROUP BY start_station_id,start_station_name
    ORDER BY total_trips DESC
    LIMIT 5
    """
    result = pd.read_sql_query(query,conn)
    return result

def top5route(conn):
    query = f"""
    SELECT 
    route,
    COUNT(route)as routefreq
    FROM(SELECT start_station_name||' - to - '||end_station_name AS route,id FROM trips)
    GROUP BY route
    ORDER BY routefreq DESC
    LIMIT 5
    """
    result = pd.read_sql_query(query,conn)
    return result


def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips VALUES {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def total_bikers(subscriber,conn):
    return pd.read_sql_query(
    f"""
    SELECT * FROM (
    SELECT id,CASE
    WHEN subscriber_type LIKE '%Local%' THEN 'Nonsubscriber'
    WHEN subscriber_type LIKE "%Student Membership%" OR subscriber_type LIKE "%Annual%" THEN 'Subscriber'
    WHEN subscriber_type IN ('3-Day Weekender','24 Hour Walk Up Pass','Single Trip (Pay-as-you-ride)','Pay-as-you-ride') THEN 'Onetime'
    ELSE subscriber_type
    END AS subscriber,
    start_time as dayname
    FROM trips ) WHERE subscriber = '{subscriber}'
    
    """,conn,parse_dates = 'dayname'
    ).assign(dayname=lambda df: df.dayname.dt.day_name()
    ).groupby(['subscriber','dayname',]).count().unstack(level=0).droplevel(0, axis = 'columns')

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection   

if __name__ == '__main__':
    app.run(debug=True, port=5000)

    