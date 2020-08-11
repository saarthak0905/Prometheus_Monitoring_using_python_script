#*************************** IMPORT STATEMENTS AND GLOBAL DECLARATIONS ****************************************
#import requests
import json
import operator
#import smtplib
import mysql.connector
from datetime import datetime
#import os
#************************** MYSQL CONNECTION ******************************************************************
try:
    mydb = mysql.connector.connect(host = 'localhost', user = 'root', passwd = 'saarthak1238', database = 'paytm_monitoring')
    mycursor = mydb.cursor()
except mysql.connector.Error as error:
    print("Could not establish mysql Connection: {}".format(error))

#************************** COST CALCULATAION FUNCTION ********************************************************
def cost_calc(endpts, metrics):
    cost = (15*endpts) + (metrics//20)
    return cost
#************************** DATA FETCHING FUNCTION ************************************************************
def data_fetch(json_path): #function takes in path of json file and returns the list of records 
    dict1 ={}
    try:
        with open(json_path) as jfile:
            dict1 = json.load(jfile)
    except IOError as e:
        print(e)
    return dict1['data']['result']
#************************** DATA IN DICTIONARIES FUNCTION *****************************************************
def data_in_dict(records, dictionary, keylabel): #'records' is the list of records fetched from prometheus.
    for record in records: #'dictionary' is the dict we want to store aur record values in. 'keylabel' can either be businessunit or techteam
        if(not bool(record['metric'])): # If the metric name is not present for that record
            key = 'N/A'
            value = int(record['value'][1]) #record['value'][0] is the timestamp and record['value'][1] contains actual value but in string format
        else:
            key = record['metric'][keylabel]
            value = int(record['value'][1])
        dictionary[key] = value #Stores key-value pair in the dictionary.
        
#************************** DICT ITERATION FUNCTION ***********************************************************
def dict_iterate(metrics, endpts, costs): #'metrics' and 'endpts' are dictionaries of Number of metrics and endpoints scrapped respectively by BUs or tech teams 
    for label1 in metrics: #label1 will take values of all BU/tech team names in metrics dictionary
        for label2 in endpts: #label1 will take values of all BU/tech team names in endpts dictionary
            if(label1 == label2): #Checking for equality in BU/tech team names in both dictionaries
                e = endpts[label2] #'e' stores value of number of endpoints scrapped for the BU/tech team
                m = metrics[label1] #'m' stores value of number of metrics scrapped for the BU/tech team
                c = cost_calc(e, m) #'c' stores the cost calculated for that BU/tech team
                costs[label1] = c # stores value of cost in 'costs' dictionary corresponding to BU/tech team name as key
#************************** DB UPDATION FUNCTION **************************************************************
def db_update(sql_query, dict1): #Takes SQL query for specific table name and dictionary to to create tuple with parameters
    now = datetime.now() #Fetches today's date
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S') #formatting date
    for key in dict1:        
        tuple_val = (key, dict1[key], formatted_date) #tuple containing parameters for sql query.
        mycursor.execute(sql_query, tuple_val)
        mydb.commit()
#************************** INTIALIZING DICTIONARIES **********************************************************
dict_BU_metrics = {}
dict_TechTeams_metrics={}
dict_BU_endpts = {}
dict_TechTeams_endpts = {}

dict_cost_BU = {}
dict_cost_TechTeams = {}
#************************** VLAUES INTO THE DICTIONARY  **********************************************************
json_bu_endpt = r"C:\Users\saart\OneDrive\Desktop\Paytm Internship\top10_endpoints_businessunit.json"
json_bu_metric = r"C:\Users\saart\OneDrive\Desktop\Paytm Internship\top10_metricsscraped_businessunit.json"
json_techteam_endpt = r"C:\Users\saart\OneDrive\Desktop\Paytm Internship\top10_endpoints_techteam.json"
json_techteam_metric = r"C:\Users\saart\OneDrive\Desktop\Paytm Internship\top10_metricscraped_techteam.json"

data_in_dict(data_fetch(json_bu_endpt), dict_BU_endpts, 'businessunit')
data_in_dict(data_fetch(json_bu_metric),dict_BU_metrics , 'businessunit')
data_in_dict(data_fetch(json_techteam_endpt), dict_TechTeams_endpts, 'techteam')
data_in_dict(data_fetch(json_techteam_metric), dict_TechTeams_metrics, 'techteam')
# #************************** SORTING AND COST CALCULATION ******************************************************


   #All dictionaries are sorted according to descending order of value.
dict_BU_metrics = dict(sorted(dict_BU_metrics.items(), key=operator.itemgetter(1),reverse=True))
dict_TechTeams_metrics = dict(sorted(dict_TechTeams_metrics.items(), key=operator.itemgetter(1),reverse=True))
dict_BU_endpts = dict(sorted(dict_BU_endpts.items(), key=operator.itemgetter(1),reverse=True))
dict_TechTeams_endpts = dict(sorted(dict_TechTeams_endpts.items(), key=operator.itemgetter(1),reverse=True))

  #next 2 lines calculate costs for BUs and tech teams and store them in dictionaries.
dict_iterate(dict_BU_metrics, dict_BU_endpts, dict_cost_BU)
dict_iterate(dict_TechTeams_metrics, dict_TechTeams_endpts, dict_cost_TechTeams)
    
dict_cost_BU = dict(sorted(dict_cost_BU.items(), key=operator.itemgetter(1),reverse=True))
dict_cost_TechTeams = dict(sorted(dict_cost_TechTeams.items(), key=operator.itemgetter(1),reverse=True))
#************************** UPDATING DATABASE *****************************************************************
try:
    sql1 = """INSERT INTO bu_endpts VALUES(%s, %s, %s)"""
    db_update(sql1, dict_BU_endpts)
    sql2 = """INSERT INTO bu_metrics VALUES(%s, %s, %s)"""
    db_update(sql2, dict_BU_metrics)
    sql3 = """INSERT INTO techteam_endpts VALUES(%s, %s, %s)"""
    db_update(sql3, dict_TechTeams_endpts)
    sql4 = """INSERT INTO techteam_metrics VALUES(%s, %s, %s)"""
    db_update(sql4, dict_TechTeams_metrics)
    sql5 = """INSERT INTO cost_bu VALUES(%s, %s, %s)"""
    db_update(sql5, dict_cost_BU)
    sql6 = """INSERT INTO cost_techteam VALUES(%s, %s, %s)"""
    db_update(sql6, dict_cost_TechTeams)
except mysql.connector.Error as error:
    print("Failed to update table records: {}".format(error))
finally:
    if (mydb.is_connected()):
        mydb.close()
        print("MySQL connection has been closed")
# #***************************************************************************************************************