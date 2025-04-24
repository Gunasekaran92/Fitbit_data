#Import Library
import requests
import json
import pandas as pd
from urllib.parse import quote
from requests.auth import HTTPBasicAuth
from requests_oauthlib import OAuth2Session
from fitbit.api import Fitbit
import fitbit
import gather_keys_oauth2 as Oauth2
import datetime as dt
import numpy as np
import random
import pyodbc
import mysql.connector
import os
from psycopg2 import pool
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


#Get Clent Id and Secrets
ClientID="****"
ClientSecret ="*****"
OAuthTokenURL='https://api.fitbit.com/oauth2/token'
RedirectURL='https://www.fitbit.com/in-app/today/'

def auth(ClientID,ClientSecret):
    '''
    Funtion to authenticate the user to access the Fibit Web API
    '''

    #Cherry PY open browser for the user to authorize the app
    server=Oauth2.OAuth2Server(ClientID, ClientSecret)
    server.browser_authorize()

    #Get the access token
    ACCESS_TOKEN=str(server.fitbit.client.session.token['access_token'])

    #get the refresh token
    REFRESH_TOKEN=str(server.fitbit.client.session.token['refresh_token'])
    auth2_client=fitbit.Fitbit(ClientID,ClientSecret,oauth2=True,access_token=ACCESS_TOKEN,refresh_token=REFRESH_TOKEN)

    return ACCESS_TOKEN




def get_req(url,token):
    '''
    Funtion to get Fitbit data
    '''

    
    #API call
    header={'Authorization' : 'Bearer {}'.format(token),
       'Content-Type' : 'application/json'}
    response=requests.get(url,headers=header).json()
    #response=requests.get(url,headers=header)

    print("Activity response initiated")

    return response



def activity(scope,activity,token):
    '''
    Activity type
    '''

    print("Activity selected : ",activity)
    #Sleep
    if activity=='sleep':
        URL="https://api.fitbit.com/1.2/user/-/sleep/list.json?afterDate=2024-04-01&sort=asc&offset=0&limit=100"
        response=get_req(URL,token)
        data=create_df(scope,response)

    #Heartrate
    elif activity=='heartrate':
        URL="https://api.fitbit.com/1/user/-/activities/heart/date/2024-04-01/2025-04-22.json"
        response=get_req(URL,token)
        data=json_serialize(scope,response)

    #Steps calculator    
    elif activity=='steps' :
        URL="https://api.fitbit.com/1/user/-/activities/steps/date/2024-04-01/2025-04-22.json"
        response=get_req(URL,token)
        data=create_df(scope,response)


    #ActiveZone
    elif activity=='active_zone':
        URL="https://api.fitbit.com/1/user/-/activities/active-zone-minutes/date/2024-04-01/2025-04-22.json"
        response=get_req(URL,token)
        data=json_serialize(scope,response)

    #Activity log
    elif activity=='activity_log':
        URL="https://api.fitbit.com/1/user/-/activities/list.json?afterDate=2024-04-01&sort=asc&offset=0&limit=100"
        response=get_req(URL,token)
        data=create_df(scope,response)

    
    return data



def create_df(scope,response):
    '''
    Create a datafarme from API response
    '''

    print("Datafarme created")
    data=response[scope]
    df=pd.DataFrame(data)

    return df

def json_serialize(scope,response):
    '''
    Serialize JSON
    '''
    print("Serialze Datafarme created")
    data=response[scope]
    df=pd.json_normalize(data)

    return df



#Authenticate 
access_token=auth(ClientID,ClientSecret) #Run this code once*



#API call and store the data in a dataframe 
steps=activity('activities-steps','steps',access_token)
heartrate=activity('activities-heart','heartrate',access_token)
sleep=activity('sleep','sleep',access_token)
active_zone=activity('activities-active-zone-minutes','active_zone',access_token)
activity_log=activity('activities','activity_log',access_token)



#data manipulation
#Steps data manipulation
steps.rename(columns={'value':'step_count'},inplace=True)
steps['dateTime']=pd.to_datetime(steps['dateTime'])

#Heart rate data manipulation
heartrate.columns
heartrate.rename(columns={'value.restingHeartRate':'heart_rate'},inplace=True)
heartrate.drop(columns=['value.customHeartRateZones','value.heartRateZones'],inplace=True)
heartrate['dateTime']=pd.to_datetime(heartrate['dateTime'])

#Sleep data manipulation
sleep.columns
sleep.drop(columns=['startTime', 'endTime','infoCode', 'isMainSleep', 'levels', 'logId','logType', 'type'],inplace=True)
sleep.rename(columns={'dateOfSleep':'dateTime','duration':'sleep_duration'},inplace=True)

#Conevert Sleep duration in ms to H:M:S
sleep['sleep_duration']=pd.to_datetime(sleep['sleep_duration'],unit='ms')
sleep['sleep_duration']=sleep['sleep_duration'].dt.strftime('%H:%M:%S.%f')
sleep['dateTime']=pd.to_datetime(sleep['dateTime'])
#sleep.head()

#Active zone
active_zone.columns
active_zone.rename(columns={'value.activeZoneMinutes':'activeZoneMinutes', 'value.fatBurnActiveZoneMinutes':'fatBurnActiveZoneMinutes','value.cardioActiveZoneMinutes':'cardioActiveZoneMinutes', 'value.peakActiveZoneMinutes':'peakActiveZoneMinutes'},inplace=True)
active_zone['dateTime']=pd.to_datetime(active_zone['dateTime'])

#Activity log
activity_log=activity_log[['startTime','activityName', 'calories', 'steps','averageHeartRate', 'duration',  'elevationGain', 'distance', 'speed', 'distanceUnit', 'pace','hasGps']]
activity_log['startTime']=pd.to_datetime(activity_log['startTime'])
activity_log['dateTime']=activity_log['startTime'].dt.date
activity_log.drop(columns=['startTime'],inplace=True)
activity_log['duration']=pd.to_datetime(activity_log['duration'],unit='ms')
activity_log['duration']=activity_log['duration'].dt.strftime('%H:%M:%S')
activity_log['dateTime']=pd.to_datetime(activity_log['dateTime'])

#join all th data with the key 'dateTime'
fitbit_join=pd.concat([pd.concat([pd.concat([pd.concat([activity_log,steps],axis=1),active_zone],axis=1),sleep],axis=1),heartrate],axis=1)
dup_cols=fitbit_join.columns[fitbit_join.columns.duplicated()].tolist()
fitbit_final=fitbit_join.loc[:,~fitbit_join.columns.duplicated()]
drop=fitbit_final.dropna(thresh=5)
drop=drop.fillna(0)



##Insert data into Postgre SQl
datawarehouse_db_config={

    
    'user': '****',
    'password':'****',
    'neon_hostname':'****.azure.neon.tech',
    'dbname':'*****'
    
}


#Create table
def db_connect(create_sql,fitbit_final):


    #Database connection string
    DATABASE_URL='postgresql://'+datawarehouse_db_config['user']+':'+datawarehouse_db_config['password']+'@'+datawarehouse_db_config['neon_hostname']+'/'+datawarehouse_db_config['dbname']+'?sslmode=require'
    #Load env file
    load_dotenv()
    #Get the connection string from env variable
    connection_string=os.getenv(DATABASE_URL)
    #Create a connection pool
    connection_pool=pool.SimpleConnectionPool(1,10,DATABASE_URL)
    #check if pool was created successfully
    if connection_pool:
        print("Connection pool created successfully")
    #Get a connection from the pool
    conn=connection_pool.getconn()
    cur=conn.cursor()
    try:
        with conn.cursor() as cur:
            #Create a cursor object
        #cur.execute(create_sql)
            for row in fitbit_final.itertuples(index=False):
                cur.execute(create_sql,row)
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
    conn.commit()
    #close the cursor and return to the connection pool
    cur.close()
    connection_pool.putconn(conn)
    #close all the connection in pool
    connection_pool.closeall()
    return cur


create_sql="""CREATE TABLE fitbit_data(
startTime timestamp,
activityName varchar(255),
calories float4,
steps float4,
averageHeartRate float4,
duration time ,
elevationGain float4,
distance float4,
speed float4,
distanceUnit float4,
pace float4,
hasGps float4,
dateTime date,
activeZoneMinutes time ,
fatBurnActiveZoneMinutes time,
cardioActiveZoneMinutes float4,
peakActiveZoneMinutes float4, 
sleep_duration float4,
efficiency float4,
minutesAfterWakeup time,
minutesAwake time, 
minutesAsleep time,
minutesToFallAsleep time,
timeInBed time,
heart_rate float4,
step_count float4)
"""
db_connect(create_sql)

insert_sql = "INSERT INTO demo2(activityName, calories, steps, averageHeartRate, duration,\
       elevationGain, distance, speed, distanceUnit, pace, hasGps,\
       dateTime, step_count, activeZoneMinutes,\
       fatBurnActiveZoneMinutes, cardioActiveZoneMinutes,\
       peakActiveZoneMinutes, sleep_duration, efficiency,\
       minutesAfterWakeup, minutesAwake, minutesAsleep,\
       minutesToFallAsleep, timeInBed, heart_rate) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *"


#Insert the data into Postgre SQL
ret=db_connect(insert_sql,drop)
