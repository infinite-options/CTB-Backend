#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import re
import json
from selenium import webdriver 
from selenium.webdriver.chrome.service import Service as ChromeService 
from webdriver_manager.chrome import ChromeDriverManager 
import pymysql
import pandas as pd
import mysql.connector
import re
import requests


# In[2]:


def dbconnect():
    conn = pymysql.connect(
        host='io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com',
        port=3306,
        user='admin',
        password='prashant',
        database='pmctb'
    )
    return conn


# In[3]:


def disconnect(conn):
    try:
        conn.close()
    except:
        print("Could not properly disconnect from MySQL database.")


# In[17]:


def insert_into_table(df, table):
    try:
        conn=dbconnect()
        columns= ", ".join([f"`{col}`" for col in df.columns])
        placeholders = ", ".join(["%s"] * len(df.columns))
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        values = [tuple(row) for row in df.values]
        with conn.cursor() as cursor:
            cursor.executemany(sql, values)
            conn.commit()
    except pymysql.Error as e:
        print("could not close connection error pymysql %d: %s" %(e.args[0], e.args[1]))
    finally:
        disconnect(conn)


# In[18]:


def googlesearchscrape():
    options = Options()
    options.headless = True
    options.add_argument("--window-size=1920,1080")
    options.add_argument("start-maximized")
    options.add_experimental_option( "prefs", {'protocol_handler.excluded_schemes.tel': False})

    #"326628762"
    model_number= input("Enter the product search string: ")
    pagetitles= []

    suppliername=[]

    externallinks={}

    currpageprice=[]

    currpagelinks=[]

    shippingcost=[]
    totalcost=[]

    deliverydate=[]
    browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options) 
    base_url= f"https://www.google.com/search?q={model_number}&sa=X&biw=1512&bih=834&tbm=shop&sxsrf=APwXEddHOqWywnO66EIdSAziJbjmQIKduQ%3A1685309310841&ei=fsdzZJDTMvaA0PEP3MCLoAo&ved=0ahUKEwiQiry2-pj_AhV2ADQIHVzgAqQQ4dUDCAg&uact=5&oq=326628762&gs_lcp=Cgtwcm9kdWN0cy1jYxADMgcIIxCwAxAnMgsIrgEQygMQsAMQJzILCK4BEMoDELADECcyCwiuARDKAxCwAxAnMgsIrgEQygMQsAMQJzILCK4BEMoDELADECcyCwiuARDKAxCwAxAnUABYAGCiBmgAcAB4AIABP4gBP5IBATGYAQDAAQHIAQc&sclient=products-cc"
    browser.get(base_url)

    mainelement= browser.find_elements(By.XPATH, "//div[@class='sh-dgr__content']")
    
    for i in mainelement:
        title= i.find_elements(By.XPATH, ".//h3[@class='tAxDx']")
        if not title:
            continue
        match= title[0].text
        testval=match

        if model_number.lower() in (testval.replace("-", "")).lower() or model_number.lower() in (testval.replace("-", " ")).lower():
            pagetitles.append(match)

            ex= i.find_elements(By.XPATH, ".//a[@class='iXEZD']")
            if len(ex)>0:
                externallinks[match]= ex[0].get_attribute("href")

            price= i.find_elements(By.XPATH, ".//span[@class='QIrs8']/span")
            currpageprice.append((price[0].text)[1:-1])

            links= i.find_elements(By.XPATH, ".//a[@class='shntl']")
            currpagelinks.append(links[0].get_attribute("href"))

            suppname= i.find_elements(By.XPATH, ".//div[@class='aULzUe IuHnof']")
            suppliername.append(suppname[0].text)

            ship= i.find_elements(By.XPATH, ".//div[@class='vEjMR']")
            if 'Free' in ship[0].text:
                shippingcost.append("0")
            elif '$' in ship[0].text:
                val= ship[0].text
                start_index = val.find('$')
                if start_index != -1:
                    end_index = start_index + 1
                    while end_index < len(val) and (val[end_index].isdigit() or val[end_index] == '.'):
                        end_index += 1

                shipprice = val[start_index + 1:end_index]

                shippingcost.append(shipprice)

            else:
                shippingcost.append("N/A")

            if "delivery" in ship[0].text and len((ship[0].text).split(" "))>2 and "Google" not in ship[0].text:
                tempobj=(ship[0].text).split(" ")[-2:]
                deliverydate.append(" ".join(tempobj))
            else:
                deliverydate.append("N/A")

            pattern = r"^[-+]?[0-9]*\.?[0-9]+$"

            match = re.match(pattern, shippingcost[-1])

            if match:
                try:
                    tc= float(shippingcost[-1])+float(currpageprice[-1])
                    totalcost.append(tc)
                except:
                    newcurr= currpageprice[-1][1:-1].split(" ")
                    tc= float(shippingcost[-1])+float(newcurr[0])
                    totalcost.append(tc)
            else:
                totalcost.append("N/A")
                
    browser.close()
    
    df= pd.DataFrame({'Description': pagetitles, 'Page Type': 'Front' , 'Seller':suppliername,
                  'Price':currpageprice, 'Shipping': shippingcost ,'Total': totalcost, 'WebURL': currpagelinks,
                 'Delivery Date':deliverydate})
    
    options = Options()
    options.headless = True
    options.add_argument("--window-size=1920,1080")
    options.add_argument("start-maximized")
    options.add_experimental_option( "prefs", {'protocol_handler.excluded_schemes.tel': False})

    browser = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
    pagetitles=[]
    varsupp= []
    supplink=[]
    prices=[]
    itemprices=[]
    shippingcost=[]
    deldate=[]

    for k,v in externallinks.items():

        base_url= v
        browser.get(base_url)
        t1= browser.find_elements(By.XPATH, "//tr[@class='sh-osd__offer-row']")


        for i in t1:
            pagetitles.append(k)
            extsupp= i.find_elements(By.XPATH, ".//a[@class='b5ycib shntl']")
            if len(extsupp)==0:
                extsupp= i.find_elements(By.XPATH, ".//div[@class='kjM2Bf']")
                varsupp.append(extsupp[0].text)
                supplink.append("Add to cart")
            else:
                varsupp.append(extsupp[0].text)
                supplink.append(extsupp[0].get_attribute('href'))


            specialoffer= i.find_elements(By.XPATH, ".//td[@class='SH30Lb yGibJf']/div")
            if 'Free' in specialoffer[0].text:
                shippingcost.append("0")
            elif '$' in specialoffer[0].text:
                val= specialoffer[0].text
                start_index = val.find('$')
                if start_index != -1:
                    end_index = start_index + 1
                    while end_index < len(val) and (val[end_index].isdigit() or val[end_index] == '.'):
                        end_index += 1

                shipc = val[start_index + 1:end_index]
                shippingcost.append(shipc)
            else:
                shippingcost.append("N/A")

            if 'by' in specialoffer[0].text:
                tempobj=(specialoffer[0].text).split(" ")[-2:]
                deldate.append(" ".join(tempobj))
            else:
                deldate.append("N/A")

        itemprice=browser.find_elements(By.XPATH, "//span[@class='g9WBQb fObmGc']")

        price= browser.find_elements(By.XPATH, "//div[@class='drzWO']")



        for i in range(len(price)):
            prices.append(price[i].text)
            itemprices.append(itemprice[i].text[1:])
    browser.close()
    
    df2= pd.DataFrame({'Description': pagetitles, 'Page Type': 'Secondary' , 'Seller':varsupp,
                  'Price':itemprices, 'Shipping': shippingcost ,'Total': prices, 'WebURL': supplink,
                  'Delivery Date':deldate})
    
    
    dfnew=df.append(df2)
    dfnew.reset_index(drop=True, inplace=True)
    insert_into_table(dfnew, 'googlescrape')
    
    


# In[21]:


googlesearchscrape()

