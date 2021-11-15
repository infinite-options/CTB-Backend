# CTB BACKEND PYTHON FILE
# https://3s3sftsr90.execute-api.us-west-1.amazonaws.com/dev/api/v2/<enter_endpoint_details> for ctb


# SECTION 1:  IMPORT FILES AND FUNCTIONS
from flask import Flask, request, render_template, url_for, redirect
from flask_restful import Resource, Api
from flask_mail import Mail, Message  # used for email
# used for serializer email and error handling
from itsdangerous import URLSafeTimedSerializer, SignatureExpired, BadTimeSignature
from flask_cors import CORS

import boto3
import os.path
import io

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from urllib.parse import urlparse
import urllib.request
import base64
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
from io import BytesIO
from pytz import timezone as ptz
import pytz
from dateutil.relativedelta import relativedelta
import math

from werkzeug.exceptions import BadRequest, NotFound

from dateutil.relativedelta import *
from decimal import Decimal
from datetime import datetime, date, timedelta
from hashlib import sha512
from math import ceil
import string
import random
import os
import hashlib

# regex
import re
# from env_keys import BING_API_KEY, RDS_PW

import decimal
import sys
import json
import pytz
import pymysql
import requests
import stripe
import binascii
from datetime import datetime
import datetime as dt
from datetime import timezone as dtz
import time

import csv


# from env_file import RDS_PW, S3_BUCKET, S3_KEY, S3_SECRET_ACCESS_KEY
s3 = boto3.client('s3')


app = Flask(__name__)
cors = CORS(app, resources={r'/api/*': {'origins': '*'}})
# Set this to false when deploying to live application
app.config['DEBUG'] = True





# SECTION 2:  UTILITIES AND SUPPORT FUNCTIONS
# EMAIL INFO
#app.config['MAIL_SERVER'] = 'smtp.gmail.com'
app.config['MAIL_SERVER'] = 'smtp.mydomain.com'
app.config['MAIL_PORT'] = 465

app.config['MAIL_USERNAME'] = 'support@manifestmy.space'
app.config['MAIL_PASSWORD'] = 'Support4MySpace'
app.config['MAIL_DEFAULT_SENDER'] = 'support@manifestmy.space'


app.config['MAIL_USE_TLS'] = False
app.config['MAIL_USE_SSL'] = True
# app.config['MAIL_DEBUG'] = True
# app.config['MAIL_SUPPRESS_SEND'] = False
# app.config['TESTING'] = False
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg'])
mail = Mail(app)
s = URLSafeTimedSerializer('thisisaverysecretkey')
# API
api = Api(app)


# convert to UTC time zone when testing in local time zone
utc = pytz.utc
# These statment return Day and Time in GMT
# def getToday(): return datetime.strftime(datetime.now(utc), "%Y-%m-%d")
# def getNow(): return datetime.strftime(datetime.now(utc),"%Y-%m-%d %H:%M:%S")

# These statment return Day and Time in Local Time - Not sure about PST vs PDT
def getToday(): return datetime.strftime(datetime.now(), "%Y-%m-%d")
def getNow(): return datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")


# NOTIFICATIONS - NEED TO INCLUDE NOTIFICATION HUB FILE IN SAME DIRECTORY
# from NotificationHub import AzureNotification
# from NotificationHub import AzureNotificationHub
# from NotificationHub import Notification
# from NotificationHub import NotificationHub
# For Push notification
# isDebug = False
# NOTIFICATION_HUB_KEY = os.environ.get('NOTIFICATION_HUB_KEY')
# NOTIFICATION_HUB_NAME = os.environ.get('NOTIFICATION_HUB_NAME')

# Twilio settings
# from twilio.rest import Client

# TWILIO_ACCOUNT_SID = os.environ.get('TWILIO_ACCOUNT_SID')
# TWILIO_AUTH_TOKEN = os.environ.get('TWILIO_AUTH_TOKEN')





# SECTION 3: DATABASE FUNCTIONALITY
# RDS for AWS SQL 5.7
# RDS_HOST = 'pm-mysqldb.cxjnrciilyjq.us-west-1.rds.amazonaws.com'
# RDS for AWS SQL 8.0
RDS_HOST = 'io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com'
RDS_PORT = 3306
RDS_USER = 'admin'
RDS_DB = 'ctb'
RDS_PW="prashant"   # Not sure if I need this
# RDS_PW = os.environ.get('RDS_PW')
S3_BUCKET = "manifest-image-db"
# S3_BUCKET = os.environ.get('S3_BUCKET')
# S3_KEY = os.environ.get('S3_KEY')
# S3_SECRET_ACCESS_KEY = os.environ.get('S3_SECRET_ACCESS_KEY')


# CONNECT AND DISCONNECT TO MYSQL DATABASE ON AWS RDS (API v2)
# Connect to MySQL database (API v2)
def connect():
    global RDS_PW
    global RDS_HOST
    global RDS_PORT
    global RDS_USER
    global RDS_DB

    # print("Trying to connect to RDS (API v2)...")
    try:
        conn = pymysql.connect(host=RDS_HOST,
                               user=RDS_USER,
                               port=RDS_PORT,
                               passwd=RDS_PW,
                               db=RDS_DB,
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
        # print("Successfully connected to RDS. (API v2)")
        return conn
    except:
        print("Could not connect to RDS. (API v2)")
        raise Exception("RDS Connection failed. (API v2)")

# Disconnect from MySQL database (API v2)
def disconnect(conn):
    try:
        conn.close()
        # print("Successfully disconnected from MySQL database. (API v2)")
    except:
        print("Could not properly disconnect from MySQL database. (API v2)")
        raise Exception("Failure disconnecting from MySQL database. (API v2)")

# Execute an SQL command (API v2)
# Set cmd parameter to 'get' or 'post'
# Set conn parameter to connection object
# OPTIONAL: Set skipSerialization to True to skip default JSON response serialization
def execute(sql, cmd, conn, skipSerialization=False):
    response = {}
    print("==> Execute Query: ", cmd)
    # print("==> Execute Query: ", cmd,sql)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            if cmd == 'get':
                result = cur.fetchall()
                response['message'] = 'Successfully executed SQL query.'
                # Return status code of 280 for successful GET request
                response['code'] = 280
                if not skipSerialization:
                    result = serializeResponse(result)
                response['result'] = result
            elif cmd == 'post':
                conn.commit()
                response['message'] = 'Successfully committed SQL command.'
                # Return status code of 281 for successful POST request
                response['code'] = 281
            else:
                response['message'] = 'Request failed. Unknown or ambiguous instruction given for MySQL command.'
                # Return status code of 480 for unknown HTTP method
                response['code'] = 480
    except:
        response['message'] = 'Request failed, could not execute MySQL command.'
        # Return status code of 490 for unsuccessful HTTP request
        response['code'] = 490
    finally:
        # response['sql'] = sql
        return response

# Serialize JSON
def serializeResponse(response):
    try:
        for row in response:
            for key in row:
                if type(row[key]) is Decimal:
                    row[key] = float(row[key])
                elif (type(row[key]) is date or type(row[key]) is datetime) and row[key] is not None:
                # Change this back when finished testing to get only date
                    row[key] = row[key].strftime("%Y-%m-%d")
                    # row[key] = row[key].strftime("%Y-%m-%d %H-%M-%S")
                # elif is_json(row[key]):
                #     row[key] = json.loads(row[key])
                elif isinstance(row[key], bytes):
                    row[key] = row[key].decode()
        return response
    except:
        raise Exception("Bad query JSON")


# RUN STORED PROCEDURES

        # MOVE STORED PROCEDURES HERE


# Function to upload image to s3
def allowed_file(filename):
    # Checks if the file is allowed to upload
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def helper_upload_img(file):
    bucket = S3_BUCKET
    # creating key for image name
    salt = os.urandom(8)
    dk = hashlib.pbkdf2_hmac('sha256',  (file.filename).encode(
        'utf-8'), salt, 100000, dklen=64)
    key = (salt + dk).hex()

    if file and allowed_file(file.filename):

        # image link
        filename = 'https://s3-us-west-1.amazonaws.com/' \
                   + str(bucket) + '/' + str(key)

        # uploading image to s3 bucket
        upload_file = s3.put_object(
            Bucket=bucket,
            Body=file,
            Key=key,
            ACL='public-read',
            ContentType='image/jpeg'
        )
        return filename
    return None

# Function to upload icons
def helper_icon_img(url):

    bucket = S3_BUCKET
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        raw_data = response.content
        url_parser = urlparse(url)
        file_name = os.path.basename(url_parser.path)
        key = 'image' + "/" + file_name

        try:

            with open(file_name, 'wb') as new_file:
                new_file.write(raw_data)

            # Open the server file as read mode and upload in AWS S3 Bucket.
            data = open(file_name, 'rb')
            upload_file = s3.put_object(
                Bucket=bucket,
                Body=data,
                Key=key,
                ACL='public-read',
                ContentType='image/jpeg')
            data.close()

            file_url = 'https://%s/%s/%s' % (
                's3-us-west-1.amazonaws.com', bucket, key)

        except Exception as e:
            print("Error in file upload %s." % (str(e)))

        finally:
            new_file.close()
            os.remove(file_name)
    else:
        print("Cannot parse url")

    return file_url





# RUN STORED PROCEDURES


def get_new_productUID(conn):
    newProductQuery = execute("call pmctb.new_product_uid();", 'get', conn)
    if newProductQuery['code'] == 280:
        return newProductQuery['result'][0]['new_id']
    return "Could not generate new product UID", 500


#  -----------------------------------------  PROGRAM ENDPOINTS START HERE  -----------------------------------------



# IMPORT STRUCTURED BOM TABLE (ASSUMES STRUCTURED BOM WHERE FIRST ROW IS HEADER WITH LEVEL, PART NUMBER, QTY AND SECOND ROW HAS TOP LEVEL ASSEMBLY.  NO OTHER COMMENTS OR INFO)
class ImportJSONBOM(Resource):
    def post(self):
        print("\nIn Import BOM")
        response = {}
        items = {}
        try:
            conn = connect()
            filepath = request.form.get('filepath')
            print(filepath)

            with open(filepath) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                print('Read csv file')
                
                # Initialize uber List
                data = []
                jsondata = []
                parents = []
                children = []

                # Bring in each row as a List and append it to the uber List
                for row in csv_reader:
                    data.append(row)

                # ONLY FOR DEBUG: Print each Row within the uber List
                # print('\n Data Table')
                # for items in data:
                #     print(data.index(items), items)


                # FIND LFT AND RGT FOR EACH ITEM IN LIST
                print('\nFINDING LFT')
                currentLevel = 0
                lft = 0
                rgt = 0

                for items in data:
                    # print("\nStarting on New Row")
                    # print(data.index(items), type(data.index(items)), items)
                    previousLevel = currentLevel
                    # print("Previous Level: ", previousLevel, type(previousLevel))
                    previouslft = lft
                    # print("Previous lft: ", previouslft, type(previouslft))
                    previousrgt = rgt
                    # print("Previous rgt: ", previousrgt, type(previousrgt))

                
                    # If it is the zeroth element (ie Header Row) then set headers
                    if data.index(items) == 0:
                        # print(data[data.index(items)])

                        # Find Index for Level
                        if 'Level' in items:
                            levelIndex = items.index('Level')
                            print("Level Index: ", levelIndex)
                        elif 'LEVEL' in items:
                            levelIndex = items.index('LEVEL')
                            print("Level Index: ", levelIndex)
                        else:
                            print("Level does not exist")
                            return("Level does not exist")


                        # Find Index for Part Number
                        if 'PART NUMBER' in items:
                            PNIndex = items.index('PART NUMBER')
                            print("PN Index: ", PNIndex)
                        elif 'NUMBER' in items:
                            PNIndex = items.index('NUMBER')
                            print("PN Index: ", PNIndex)
                        elif 'Number' in items:
                            PNIndex = items.index('Number')
                            print("PN Index: ", PNIndex)
                        elif 'Part' in items:
                            PNIndex = items.index('Part')
                            print("PN Index: ", PNIndex)
                        elif 'GPN' in items:
                            PNIndex = items.index('GPN')
                            print("PN Index: ", PNIndex)
                        else:
                            print("PN does not exist")
                            PNIndex = -1
                        

                        # Find Index for QTY
                        if 'qty' in items:
                            QtyIndex = items.index('qty')
                            print("Qty Index: ", QtyIndex)
                        elif 'Qty' in items:
                            QtyIndex = items.index('Qty')
                            print("Qty Index: ", QtyIndex)
                        elif 'QTY' in items:
                            QtyIndex = items.index('QTY')
                            print("Qty Index: ", QtyIndex)
                        elif 'Qty Per' in items:
                            QtyIndex = items.index('Qty Per')
                            print("Qty Index: ", QtyIndex)
                        else:
                            print("Qty does not exist")
                            return("Qty does not exist")


                        items.extend(['lft', 'rgt', 'Parent'])
                        # print(data.index(items), items)
                        
                        lftIndex = items.index('lft')
                        rgtIndex = items.index('rgt')
                        parentIndex = items.index('Parent')
                        # print(levelIndex, lftIndex, rgtIndex, parentIndex, type(parentIndex))


                    # If it is the first element (ie Top Level Assembly) then set lft and rgt
                    elif data.index(items) == 1:
                        # print(data[data.index(items)])
                        currentLevel = 0
                        # print("Current Level: ", currentLevel, type(currentLevel))
                        lft = data.index(items)
                        rgt = 0
                        items.extend([lft, rgt, 'Parent']) 
                        # print(items)
                        # print(data.index(items), items)
                        

                    # For the rest of the tree calc lft and rgt
                    elif data.index(items) > 1:
                        currentLevel = int(items[levelIndex])
                        # print("Current Level: ", currentLevel, type(currentLevel))
                        levelDiff = currentLevel - previousLevel
                        # print("Level Difference: ", levelDiff, type(levelDiff))
                        if levelDiff ==  1:
                            lft = previouslft + 1
                        elif levelDiff ==  0:
                            lft = previouslft + 2
                        elif levelDiff == -1:
                            lft = previouslft + 3
                        elif levelDiff == -2:
                            lft = previouslft + 4
                        elif levelDiff == -3:
                            lft = previouslft + 5       
                        elif levelDiff == -4:
                            lft = previouslft + 6
                        else:
                            print("Need to expand range")
                        # print("New lft: ", lft, type(lft))

                        rgt = 0
                        items.extend([lft, rgt, 'Parent']) 
                        # ONLY FOR DEBUG: PRINT RESULTANT TABLE WITH LFT VALUES
                        # print(data.index(items), items)

                # Level MUST BE IN COLUMN 2
                print('\nFINDING RGT')
                currentLevel = 0
                lft = 0
                rgt = 0
                # print(levelIndex, lftIndex)

                for items in data[1:]:
                    currentRow = int(data.index(items))
                    currentLevel = int(items[levelIndex])
                    lft = int(items[lftIndex])
                    print("\nCurrent Level: ", currentLevel, "Current LFT: ", lft, "Current Row: ", currentRow, type(currentRow))
                    
                    # If it is the first element (ie Top Level Assembly) then set lft and rgt
                    if currentRow == 1:
                        print(data[currentRow])
                        # currentLevel = 0
                        # print("Current Level: ", currentLevel, type(currentLevel))
                        rgt = 2 * (len(data) -1)
                        print("RGT is: ", rgt)
                        
                    # Check if this is the last row
                    elif currentRow == len(data) - 1:
                        # print("Last Row")
                        print(data[currentRow])
                        rgt = lft + 1
                        print("RGT is: ", rgt)
                        
                        
                    # Check if this level is deeper than the next level
                    elif currentLevel >= int(data[currentRow + 1][levelIndex]):
                        # print("Last Child before Next Assembly")
                        print(data[currentRow])
                        rgt = lft + 1
                        # print("RGT is: ", rgt)
                        

                    # If current level is above next level then find next match
                    else:
                        print("In Assembly")
                        print(data[currentRow])
                        # Compare to next rows
                        for remainingItems in data[(currentRow + 1):]:
                            print("Current Level: ", currentLevel, "Next Level: ", int(remainingItems[levelIndex]))
                            if int(remainingItems[levelIndex]) <= currentLevel:
                                print("End Assembly")
                                print(remainingItems[lftIndex])
                                levelDiff = int(remainingItems[levelIndex]) - currentLevel
                                print(levelDiff)
                                rgt = int(remainingItems[lftIndex]) - 1 + int(levelDiff)
                                print("RGT is: ", rgt)
                                break
                            
                            else:
                                print("No Match")
                                rgt = 2 * (len(data) -1) - currentLevel
                                print("RGT is: ", rgt)

                    print("New rgt: ", rgt, type(rgt))

                    items[rgtIndex] = rgt
                    print(rgt, lft, PNIndex, items[PNIndex])
                    print(int(data[currentRow][levelIndex]))
                    print(data[currentRow][PNIndex])
                    print(data[currentRow][QtyIndex])
                    print(lft, rgt, 'Parent')
                    if (rgt == lft + 1 and PNIndex != -1):
                        print("in Child")
                        items[parentIndex] = 'Child'
                        jsondata.extend([{'Level':int(data[currentRow][levelIndex]), 'PN':data[currentRow][PNIndex], 'Qty':data[currentRow][QtyIndex], 'lft':lft, 'rgt':rgt, 'Parent':'Child'}])
                        if (items[PNIndex] not in children):
                            children.append(items[PNIndex])
                       
                    elif (rgt != lft + 1 and PNIndex != -1):
                        print("in Parent")
                        jsondata.extend([{'Level':int(data[currentRow][levelIndex]), 'PN':data[currentRow][PNIndex], 'Qty':data[currentRow][QtyIndex], 'lft':lft, 'rgt':rgt, 'Parent':'Parent'}])
                        if (items[PNIndex] not in parents):
                            parents.append(items[PNIndex])
                        
                    # print(data.index(items), items)

                # ONLY FOR DEBUG: PRINT RESULTANT TABLE IN READABLE FORMAT
                # Print each Row within the uber List
                # print('\n Resulting Data Table')
                # for items in data:
                #     print(data.index(items), items)
                    

                dataBOM = json.dumps(data)
                # print(dataBOM)
                jsonBOM = json.dumps(jsondata)
                # print(jsondata)
                jsonparents = json.dumps(parents)
                # print("parents: ", parents)
                jsonchildren = json.dumps(children)
                # print("children: ", children)
                

            # Call stored procedure to get new product UID


            new_product_uid = get_new_productUID(conn)
            print(new_product_uid)
            print(getNow())
            product_desc = filepath

            # Run query to enter new product UID and BOM into table
            productquery =  '''
                INSERT INTO pmctb.products
                SET product_uid = \'''' + new_product_uid + '''\',
                    product_created = \'''' + getNow() + '''\',
                    product_desc = \'''' + product_desc + '''\',
                    product_BOM = \'''' + jsonBOM + '''\',
                    product_parents = \'''' + jsonparents + '''\',
                    product_children = \'''' + jsonchildren + '''\'
                '''

            items = execute(productquery, "post", conn)
            print("items: ", items)

            

            return(new_product_uid)
        except:
            print("Something went wrong")
        finally:
            disconnect(conn)








# IMPORT STRUCTURED BOM TABLE (ASSUMES STRUCTURED BOM WHERE FIRST ROW IS HEADER WITH LEVEL, PART NUMBER, QTY AND SECOND ROW HAS TOP LEVEL ASSEMBLY.  NO OTHER COMMENTS OR INFO)
# class ImportFileBOM(Resource):
#     def post(self):
#         print("\nIn Import File")
#         response = {}
#         items = {}
#         try:
#             conn = connect()
                    
#             # Initialize uber List
#             data = []

#             filepath = request.files['filepath']
#             stream = io.StringIO(filepath.stream.read().decode("UTF8"), newline=None)
#             # print(filepath.filename)

#             csv_input = csv.reader(stream)
#             for row in csv_input:
#                 data.append(row)
#                 # print(row)

#             product = TraverseTable(data, filepath.filename)

#             return(product)

#         except:
#             print("Something went wrong")
#         finally:
#             disconnect(conn)



class ImportFile(Resource):
    def post(self):
        print("\nIn Import File")
        response = {}
        items = {}
        try:
            conn = connect()
                    
            # Initialize uber List
            data = []

            filepath = request.files['filepath']
            stream = io.StringIO(filepath.stream.read().decode("UTF8"), newline=None)
            print(filepath.filename)

            csv_input = csv.reader(stream)
            for row in csv_input:
                data.append(row)
                # print(row)

            product = TraverseTable(data, filepath.filename)
            print('Back in Class')
            return(product)

        except:
            print("Something went wrong")
        finally:
            disconnect(conn)


# Same as ImportFile but accepts a filepath.  Only works via Postman
class ImportPath(Resource):
    def post(self):
        print("\nIn Import Path")
        response = {}
        items = {}
        try:
            conn = connect()

            # Initialize uber List
            data = []

            filepath = request.form.get('filepath')
            print(filepath)

            with open(filepath) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                print(filepath)
                
                # Bring in each row as a List and append it to the uber List
                for row in csv_reader:
                    data.append(row)
                    # print(row)

            product = TraverseTable(data, filepath)

            return(product)

        except:
            print("Something went wrong")
        finally:
            disconnect(conn)




def TraverseTable(file, filename):
    print("\nIn Traverse Table")
    response = {}
    items = {}
    try:
        conn = connect()

        jsondata = []
        parents = []
        children = []
        tree = {}
        last_level_row = {}
        
        # ONLY FOR DEBUG: Print each Row within the uber List
        print('\n Data Table')
        for items in file:
            print(file.index(items), items)


        # FIND LFT AND RGT FOR EACH ITEM IN LIST
        print('\nFINDING LFT')
        currentLevel = 0
        lft = 0
        rgt = 0

        # FIND LFT
        for items in file:
            # print("\nStarting on New Row")
            # print(data.index(items), type(data.index(items)), items)
            previousLevel = currentLevel
            # print("Previous Level: ", previousLevel, type(previousLevel))
            previouslft = lft
            # print("Previous lft: ", previouslft, type(previouslft))
            previousrgt = rgt
            # print("Previous rgt: ", previousrgt, type(previousrgt))

            
            # If it is the zeroth element (ie Header Row) then set headers
            if file.index(items) == 0:
                # print(data[data.index(items)])

                # Find Index for Level
                if 'Level' in items:
                    levelIndex = items.index('Level')
                    print("Level Index: ", levelIndex)
                elif 'LEVEL' in items:
                    levelIndex = items.index('LEVEL')
                    print("Level Index: ", levelIndex)
                else:
                    print("Level does not exist")
                    return("Level does not exist")


                # Find Index for Part Number
                if 'PART NUMBER' in items:
                    PNIndex = items.index('PART NUMBER')
                    print("PN Index: ", PNIndex)
                elif 'NUMBER' in items:
                    PNIndex = items.index('NUMBER')
                    print("PN Index: ", PNIndex)
                elif 'Number' in items:
                    PNIndex = items.index('Number')
                    print("PN Index: ", PNIndex)
                elif 'Part' in items:
                    PNIndex = items.index('Part')
                    print("PN Index: ", PNIndex)
                elif 'GPN' in items:
                    PNIndex = items.index('GPN')
                    print("PN Index: ", PNIndex)
                else:
                    print("PN does not exist")
                    PNIndex = -1
                

                # Find Index for QTY
                if 'qty' in items:
                    QtyIndex = items.index('qty')
                    print("Qty Index: ", QtyIndex)
                elif 'Qty' in items:
                    QtyIndex = items.index('Qty')
                    print("Qty Index: ", QtyIndex)
                elif 'QTY' in items:
                    QtyIndex = items.index('QTY')
                    print("Qty Index: ", QtyIndex)
                elif 'Qty Per' in items:
                    QtyIndex = items.index('Qty Per')
                    print("Qty Index: ", QtyIndex)
                else:
                    print("Qty does not exist")
                    return("Qty does not exist")


                items.extend(['lft', 'rgt', 'Parent'])
                # print(data.index(items), items)
                
                lftIndex = items.index('lft')
                rgtIndex = items.index('rgt')
                parentIndex = items.index('Parent')
                # print(levelIndex, lftIndex, rgtIndex, parentIndex, type(parentIndex))

            
            # If it is the first element (ie Top Level Assembly) then set lft and rgt
            elif file.index(items) == 1:
                # print(data[data.index(items)])
                currentLevel = 0
                # print("Current Level: ", currentLevel, type(currentLevel))
                lft = file.index(items)
                rgt = 0
                items.extend([lft, rgt, 'Parent']) 
                # print(items)
                # print(data.index(items), items)
                

            # For the rest of the tree calc lft and rgt
            elif file.index(items) > 1:
                currentLevel = int(items[levelIndex])
                # print("Current Level: ", currentLevel, type(currentLevel))
                levelDiff = currentLevel - previousLevel
                # print("Level Difference: ", levelDiff, type(levelDiff))
                if levelDiff ==  1:
                    lft = previouslft + 1
                elif levelDiff ==  0:
                    lft = previouslft + 2
                elif levelDiff == -1:
                    lft = previouslft + 3
                elif levelDiff == -2:
                    lft = previouslft + 4
                elif levelDiff == -3:
                    lft = previouslft + 5       
                elif levelDiff == -4:
                    lft = previouslft + 6
                else:
                    print("Need to expand range")
                # print("New lft: ", lft, type(lft))

                rgt = 0
                items.extend([lft, rgt, 'Parent']) 
                # ONLY FOR DEBUG: PRINT RESULTANT TABLE WITH LFT VALUES
                # print(data.index(items), items)


        # FIND RGT
        print('\nFINDING RGT')
        currentLevel = 0
        lft = 0
        rgt = 0
        # print(levelIndex, lftIndex)

        for items in file[1:]:
            currentRow = int(file.index(items))
            currentLevel = int(items[levelIndex])
            lft = int(items[lftIndex])
            print("\nCurrent Level: ", currentLevel, "Current LFT: ", lft, "Current Row: ", currentRow, type(currentRow))
            
            # If it is the first element (ie Top Level Assembly) then set lft and rgt
            if currentRow == 1:
                print(file[currentRow])
                # currentLevel = 0
                # print("Current Level: ", currentLevel, type(currentLevel))
                rgt = 2 * (len(file) -1)
                print("RGT is: ", rgt)
                
            # Check if this is the last row
            elif currentRow == len(file) - 1:
                # print("Last Row")
                print(file[currentRow])
                rgt = lft + 1
                print("RGT is: ", rgt)
                
                
            # Check if this level is deeper than the next level
            elif currentLevel >= int(file[currentRow + 1][levelIndex]):
                # print("Last Child before Next Assembly")
                print(file[currentRow])
                rgt = lft + 1
                # print("RGT is: ", rgt)
                

            # If current level is above next level then find next match
            else:
                print("In Assembly")
                print(file[currentRow])
                # Compare to next rows
                for remainingItems in file[(currentRow + 1):]:
                    print("Current Level: ", currentLevel, "Next Level: ", int(remainingItems[levelIndex]))
                    if int(remainingItems[levelIndex]) <= currentLevel:
                        print("End Assembly")
                        print(remainingItems[lftIndex])
                        levelDiff = int(remainingItems[levelIndex]) - currentLevel
                        print(levelDiff)
                        rgt = int(remainingItems[lftIndex]) - 1 + int(levelDiff)
                        print("RGT is: ", rgt)
                        break
                    
                    else:
                        print("No Match")
                        rgt = 2 * (len(file) -1) - currentLevel
                        print("RGT is: ", rgt)

            print("New rgt: ", rgt, type(rgt))

            items[rgtIndex] = rgt
            print("\nCurrent Item: ", items)
            print("Current Data: ", lft, rgt, PNIndex, items[PNIndex], items[QtyIndex])
            print("Current Row Level: ", int(file[currentRow][levelIndex]))
            print("Level Index: ", levelIndex)
            print("Current Row: ", int(items[levelIndex]))



            if currentRow == 1:
                # tree_key = f'{items[PNIndex]}'
                tree_key = f'{items[PNIndex]}-{lft}'
                tree[items[PNIndex]] = {}
                tree[items[PNIndex]][tree_key] = {}
                last_level_row[items[levelIndex]] = [items[PNIndex], tree_key]
                print("\nKey: ", tree_key)
                print("Last Level Row: ", last_level_row)
                parent = last_level_row[str(int(items[levelIndex]))][0]
                parent_left = last_level_row[str(int(items[levelIndex]))][1]
                print("Parent: ", parent, parent_left)
            else:
                tree_key = f'{items[PNIndex]}-{lft}'
                print("\nKey: ", tree_key)
            
                print("Last Level Row: ", last_level_row)
                parent = last_level_row[str(int(items[levelIndex]) - 1)][0]
                parent_left = last_level_row[str(int(items[levelIndex]) - 1)][1]
                print("Parent: ", parent, parent_left)


            if (rgt == lft + 1 and PNIndex != -1):
                print("in Child")
                items[parentIndex] = 'Child'
                # tree[last_level_row[str(int(items[levelIndex]) - 1)]][items[PNIndex]] = currentRow - 1  
                # tree[last_level_row[str(int(items[levelIndex]) - 1)]][items[PNIndex]] = {'idx':currentRow -1, 'lft':lft, 'rgt':rgt, 'qty':items[QtyIndex], 'parent': last_level_row[str(int(items[levelIndex]) - 1)] }
                tree[parent][parent_left][items[PNIndex]]                             = {'idx':currentRow -1, 'lft':lft, 'rgt':rgt, 'qty':items[QtyIndex], 'parent': last_level_row[str(int(items[levelIndex]) - 1)][0], 'parent-lft': last_level_row[str(int(items[levelIndex]) - 1)][1] }
                
                jsondata.extend([{'Level':int(items[levelIndex]), 'PN':items[PNIndex], 'Qty':items[QtyIndex], 'lft':lft, 'rgt':rgt, 'Parent':'Child'}])
                if (items[PNIndex] not in children):
                    children.append(items[PNIndex])
                
            elif (rgt != lft + 1 and PNIndex != -1):
                print("in Parent")
                
                
                # tree[items[PNIndex]] = {}
                # tree[tree_key] = {}

                # PARTIALLY WORKING
                # print(f'{items[PNIndex]}{lft}')
                # tree[f'{items[PNIndex]}{lft}'] = {}

                # tree[f'{items[PNIndex]}{lft}'] = {}
                # A = f'{last_level_row[str(int(items[levelIndex]) - 1)]}{lft}'
                # print(A)

                # last_level_row[items[levelIndex]] = items[PNIndex]
                # last_level_row[items[levelIndex]] = tree_key

                if currentRow != 1:
                    print("Inside Current Row: ", currentRow)
                    print(last_level_row[str(int(items[levelIndex]) - 1)])

                    if items[PNIndex] not in tree:
                        tree[items[PNIndex]] = {}
                    tree[items[PNIndex]][tree_key] = {}
                    last_level_row[items[levelIndex]] = [items[PNIndex], tree_key]

                    

                    # tree[last_level_row[str(int(items[levelIndex]) - 1)]][items[PNIndex]] = currentRow - 1  
                    # tree[last_level_row[str(int(items[levelIndex]) - 1)]][items[PNIndex]] = {'idx':currentRow -1, 'lft':lft, 'rgt':rgt, 'qty':items[QtyIndex], 'parent': last_level_row[str(int(items[levelIndex]) - 1)] }
                    
                    tree[parent][parent_left][items[PNIndex]]                             = {'idx':currentRow -1, 'lft':lft, 'rgt':rgt, 'qty':items[QtyIndex], 'parent': last_level_row[str(int(items[levelIndex]) - 1)][0], 'parent-lft': last_level_row[str(int(items[levelIndex]) - 1)][1] }

                jsondata.extend([{'Level':int(items[levelIndex]), 'PN':items[PNIndex], 'Qty':items[QtyIndex], 'lft':lft, 'rgt':rgt, 'Parent':'Parent'}])
                if (items[PNIndex] not in parents):
                    parents.append(items[PNIndex])

            print("Finished one part")
                
            # print(data.index(items), items)

        # ONLY FOR DEBUG: PRINT RESULTANT TABLE IN READABLE FORMAT
        # Print each Row within the uber List
        # print('\n Resulting Data Table')
        # for items in data:
        #     print(data.index(items), items)
            

        dataBOM = json.dumps(file)
        # print(dataBOM)
        jsonBOM = json.dumps(jsondata)
        print(jsondata)
        jsonparents = json.dumps(parents)
        print("parents: ", parents)
        jsonchildren = json.dumps(children)
        print("children: ", children)
        print("last Level: ", last_level_row)
        jsontree = json.dumps(tree)
        print("tree: ", tree)
            

        # Call stored procedure to get new product UID
        new_product_uid = get_new_productUID(conn)
        print(new_product_uid)
        print(getNow())
        product_desc = filename
        print(product_desc)

        # Run query to enter new product UID and BOM into table
        productquery =  '''
            INSERT INTO pmctb.products
            SET product_uid = \'''' + new_product_uid + '''\',
                product_created = \'''' + getNow() + '''\',
                product_desc = \'''' + product_desc + '''\',
                product_BOM = \'''' + jsonBOM + '''\',
                product_parents = \'''' + jsonparents + '''\',
                product_children = \'''' + jsonchildren + '''\',
                product_tree = \'''' + jsontree + '''\',
                product_status = 'ACTIVE'
            '''

        items = execute(productquery, "post", conn)
        print("items: ", items)

        return(new_product_uid)
        
    except:
        print("Something went wrong")
    finally:
        disconnect(conn)
        



class Demand(Resource):
    def get(self):
        print("\nInside Demand")
        response = {}
        items = {}

        try:
            conn = connect()
            print("Inside try block")

            # Get All Product Data
            query = """
                    SELECT * 
                    FROM pmctb.demand;
                    """

            products = execute(query, 'get', conn)

            return products['result']
        
        except:
            raise BadRequest('Demand Request failed, please try again later.')
        finally:
            disconnect(conn)


class Inventory(Resource):
    def get(self):
        print("\nInside Inventory")
        response = {}
        items = {}

        try:
            conn = connect()
            print("Inside try block")

            # Get All Product Data
            query = """
                    SELECT * 
                    FROM pmctb.inventory;
                    """

            products = execute(query, 'get', conn)

            return products['result']
        
        except:
            raise BadRequest('Inventory Request failed, please try again later.')
        finally:
            disconnect(conn)




































class AllProducts(Resource):
    def get(self):
        print("\nInside CTB_test")
        response = {}
        items = {}

        try:
            conn = connect()
            print("Inside try block")

            # Get All Product Data
            query = """
                    SELECT * 
                    FROM pmctb.products
                    WHERE product_status != 'DELETED';
                    """

            products = execute(query, 'get', conn)

            return products['result']
        
        except:
            raise BadRequest('Products Request failed, please try again later.')
        finally:
            disconnect(conn)


class Products(Resource):
    def get(self,product_uid):
        print("\nInside CTB_test")
        response = {}
        items = {}

        try:
            conn = connect()
            print("Inside try block")

            # Get Product Specific Data
            query = """
                    SELECT * 
                    FROM pmctb.products 
                    WHERE product_uid = '""" + product_uid + """';
                    """

            products = execute(query, 'get', conn)

            return products['result']
        
        except:
            raise BadRequest('Products Request failed, please try again later.')
        finally:
            disconnect(conn)


class GetBOM(Resource):
    def post(self):
        print("\nInside GetBOM")
        response = {}
        items = {}

        try:
            conn = connect()
            print("Inside try block")
            data = request.get_json(force=True)
            print("Received:", data)
            product_uid = data["product_uid"]

            # Get Product Specific Data
            query = """
                    SELECT * 
                    FROM pmctb.products 
                    WHERE product_uid = \'""" + product_uid + """\';
                    """

            products = execute(query, 'get', conn)

            return products['result']
        
        except:
            raise BadRequest('Products Request failed, please try again later.')
        finally:
            disconnect(conn)


class RunCTB(Resource):
    def post(self):
        print("\nInside Run CTB")
        response = {}
        items = {}

        try:
            conn = connect()
            # print("Inside try block")
            data = request.get_json(force=True)
            # print("Received:", data)
            product_uid = data["product_uid"]
            print("product_uid:", product_uid)

            CreateBOMView(product_uid)
            # Get Product Specific Data
            print("\nBack in Run CTB - 2nd query")
            query = """
                    SELECT
                        grandparent_lvl.BOM_level    
                        , grandparent_lvl.BOM_pn as GrandParent_BOM_pn   
                        , grandparent_lvl.BOM_lft as gp_lft    
                        , child_lvl.BOM_pn as Child_pn 
                        , child_lvl.BOM_lft    
                        , child_lvl.BOM_qty as Qty_per
                        , round(POWER(10,Sum(Log(10,parent_lvl.BOM_qty))),2) as RequiredQty  
                    FROM    
                        pmctb.BOMView grandparent_lvl  -- Need names to distinguish tables from one another
                        , pmctb.BOMView parent_lvl 
                        , pmctb.BOMView child_lvl    
                    WHERE    
                        ((parent_lvl.BOM_lft) Between (grandparent_lvl.BOM_lft+1) And (grandparent_lvl.BOM_rgt))     
                        AND (child_lvl.BOM_lft)=child_lvl.BOM_rgt-1                                     
                        AND ((child_lvl.BOM_lft) Between (parent_lvl.BOM_lft) And (parent_lvl.BOM_rgt) )     
                    GROUP BY    
                        grandparent_lvl.BOM_level   
                        , grandparent_lvl.BOM_pn 
                        , grandparent_lvl.BOM_lft    
                        , child_lvl.BOM_pn    
                        , child_lvl.BOM_lft    
                        , child_lvl.BOM_qty    
                    ORDER BY    
                        grandparent_lvl.BOM_level
                        , grandparent_lvl.BOM_pn    
                        , child_lvl.BOM_lft 
                    """
            # print(query)
            ctb = execute(query, 'get', conn)

            return ctb['result']
        
        except:
            raise BadRequest('Run CTB failed, please try again later.')
        finally:
            disconnect(conn)



class RunOrderList(Resource):
    def post(self):
        print("\nInside Run Order List")
        response = {}
        items = {}

        try:
            conn = connect()
            # print("Inside try block")
            data = request.get_json(force=True)
            # print("Received:", data)
            product_uid = data["product_uid"]
            print("product_uid:", product_uid)

            CreateBOMView(product_uid)

            # Get Product Specific Data
            print("\nBack in Run Order List - 2nd query")
            query = """
                    SELECT 
                        BOM_level, 
                        GrandParent_BOM_pn, 
                        gp_lft, 
                        Child_pn,
                        Sum(RequiredQty) AS RequiredQty
                    FROM (
                        SELECT
                            grandparent_lvl.BOM_level    
                            , grandparent_lvl.BOM_pn as GrandParent_BOM_pn   
                            , grandparent_lvl.BOM_lft as gp_lft    
                            , child_lvl.BOM_pn as Child_pn 
                            , child_lvl.BOM_lft    
                            , child_lvl.BOM_qty as Qty_per
                            , round(POWER(10,Sum(Log(10,parent_lvl.BOM_qty))),2) as RequiredQty                 -- Magic Formula from Joe Celko
                        FROM    
                            pmctb.BOMView grandparent_lvl  -- Need names to distinguish tables from one another
                            , pmctb.BOMView parent_lvl 
                            , pmctb.BOMView child_lvl    
                        WHERE    
                            ((parent_lvl.BOM_lft) Between (grandparent_lvl.BOM_lft+1) And (grandparent_lvl.BOM_rgt))    -- Find all parts within the grandparent levels    
                            AND (child_lvl.BOM_lft)=child_lvl.BOM_rgt-1                                                 -- Find only the children    
                            AND ((child_lvl.BOM_lft) Between (parent_lvl.BOM_lft) And (parent_lvl.BOM_rgt) )            -- Find just the children that report to the parents    
                        GROUP BY    
                            grandparent_lvl.BOM_level   
                            , grandparent_lvl.BOM_pn 
                            , grandparent_lvl.BOM_lft    
                            , child_lvl.BOM_pn    
                            , child_lvl.BOM_lft    
                            , child_lvl.BOM_qty    
                        ORDER BY    
                            grandparent_lvl.BOM_level
                            , grandparent_lvl.BOM_pn    
                            , child_lvl.BOM_lft 
                        ) AS tempCTB		
                    GROUP BY Child_pn, GrandParent_BOM_pn;
                    """
            # print(query)
            ctb = execute(query, 'get', conn)

            return ctb['result']
        
        except:
            raise BadRequest('Run Order List failed, please try again later.')
        finally:
            disconnect(conn)


def CreateBOMView(product_uid):
    try:
        conn = connect()
        print("\nInside Create BOM View")
        print("pruduct_uid:", product_uid)

        # Drop BOMView
        query1 = """
                    DROP VIEW IF EXISTS pmctb.BOMView;
                """

        drop = execute(query1, 'post', conn)
        # print(drop)
        if drop["code"] == 281:
            print("BOMView dropped")
        
        # Create BOMView
        query = """
                    CREATE VIEW pmctb.BOMView AS (
                    SELECT 
                        CONCAT(product_uid, "-", BOM_id) AS BOM_uid,
                        BOM_Level,
                        BOM_pn,
                        BOM_qty,
                        BOM_lft,
                        BOM_rgt,
                        BOM_Parent
                    FROM (
                        SELECT *
                        FROM pmctb.products AS p,
                        JSON_TABLE (p.product_BOM, '$[*]'
                            COLUMNS (
                                    BOM_id FOR ORDINALITY,
                                    BOM_pn VARCHAR(255) PATH '$.PN',
                                    BOM_qty DOUBLE PATH '$.Qty',
                                    BOM_lft INT PATH '$.lft',
                                    BOM_rgt INT PATH '$.rgt',
                                    BOM_Level INT PATH '$.Level',
                                    BOM_Parent VARCHAR(255) PATH '$.Parent')
                                    ) AS BOM
                    WHERE product_uid = \'""" + product_uid + """\'
                    
                        ) AS BOM)
                    """
        # print(query)
        create = execute(query, 'get', conn)
        print("BOMView Created")
        print(create["code"])

        return
    
    except:
        raise BadRequest('BOM Engine failed, please try again later.')
    finally:
        disconnect(conn)


class Delete(Resource):
    def post(self):
        print("\nInside Delete")
        response = {}
        items = {}

        try:
            conn = connect()
            print("Inside try block")
            data = request.get_json(force=True)
            # print("Received:", data)
            product_uid = data["product_uid"]
            print("product_uid:", product_uid)

            query = """
                    UPDATE pmctb.products
                    SET product_status = 'DELETED'
                    WHERE product_uid = \'""" + product_uid + """\'
                    """

            products = execute(query, 'post', conn)
            print("Back in class")
            print(products)
            return products['code']
        
        except:
            raise BadRequest('Delete Request failed, please try again later.')
        finally:
            disconnect(conn)


class UploadFile(Resource):
    def post(self):
        print("Inside UploadFile")
        import csv
        import urllib
        response = {}
        items = {}

        try:
            conn = connect()
            upload_data = []

            f = request.files['filepath']
            stream = io.StringIO(f.stream.read().decode("UTF8"), newline=None)

            csv_input = csv.reader(stream)
            for row in csv_input:
                print(row)
                upload_data.append(row)

            return(upload_data)

        except:
            raise BadRequest('Upload File Request failed')
        finally:
            disconnect(conn)









#  -- ACTUAL ENDPOINTS    -----------------------------------------

# New APIs, uses connect() and disconnect()
# Create new api template URL
# api.add_resource(TemplateApi, '/api/v2/templateapi')

# Run on below IP address and port
# Make sure port number is unused (i.e. don't use numbers 0-1023)

# GET requests
api.add_resource(ImportJSONBOM, '/api/v2/ImportJSONBOM')
api.add_resource(ImportFile, '/api/v2/ImportFile')
api.add_resource(ImportPath, '/api/v2/ImportPath')
api.add_resource(AllProducts, "/api/v2/AllProducts")
api.add_resource(Products, "/api/v2/Products/<string:product_uid>")
api.add_resource(GetBOM, "/api/v2/GetBOM")
api.add_resource(RunCTB, "/api/v2/RunCTB")
api.add_resource(RunOrderList, "/api/v2/RunOrderList")
api.add_resource(Delete, "/api/v2/Delete")

api.add_resource(Demand, "/api/v2/Demand")
api.add_resource(Inventory, "/api/v2/Inventory")

api.add_resource(UploadFile, "/api/v2/UploadFile")


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=4000)
