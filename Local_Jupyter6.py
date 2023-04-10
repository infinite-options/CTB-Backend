#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymysql
import pandas as pd
import mysql.connector
from sqlalchemy import func


# In[2]:


import ctb_api


# In[3]:


from bs4 import BeautifulSoup
import pandas as pd
import re
import numpy as np
import json
import requests


# In[29]:


import sqlalchemy
import requests
import pandas as pd

engine = sqlalchemy.create_engine('mysql+mysqlconnector://admin:prashant@io-mysqldb8.cxjnrciilyjq.us-west-1.rds.amazonaws.com:3306/pmctb', connect_args={"connect_timeout": 500})


# In[51]:


def get_supplier_uid():
    connection = engine.connect()
    stmt = sqlalchemy.text("CALL pmctb.new_supplier_uid2()")
    result = engine.execute(stmt)
    connection.close()
    return result.fetchall()

def get_video_uid():
    connection = engine.connect()
    stmt = sqlalchemy.text("CALL pmctb.new_video_uid2()")
    result = engine.execute(stmt)
    connection.close()
    return result.fetchall()

def get_bom_uid():
    connection = engine.connect()
    stmt = sqlalchemy.text("CALL new_bom_uid2()")
    result = engine.execute(stmt)
    connection.close()
    return result.fetchall()

def get_parts_uid():
    connection = engine.connect()
    stmt = sqlalchemy.text("CALL pmctb.new_parts_uid2()")
    result = engine.execute(stmt)
    connection.close()
    return result.fetchall()

def get_inventory_uid():
    connection = engine.connect()
    stmt = sqlalchemy.text("CALL pmctb.new_inventory_uid2()")
    result = engine.execute(stmt)
    connection.close()
    return result.fetchall()


# In[6]:


def createlevels(model):
    URL= f"https://www.partselect.com/Models/{model}/"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    title= soup.find_all("h1", class_="title-main mt-3 mb-4")
    if len(title)<1:
        while len(title)<1:
            model=model[:-1]
            URL= f"https://www.partselect.com/Models/{model}/"
            page = requests.get(URL)
            soup = BeautifulSoup(page.content, "html.parser")
            title= soup.find_all("h1", class_="title-main mt-3 mb-4")
    print(title[0].text)
    app= " ".join(((title[0].text).split(" "))[1:])
    df= pd.DataFrame(columns= ['Level', 'Manufacturer Part No.', "PS No.", "Part Name","Availability", "Price", "Manufacturer name"])
    uniquestr= app.split(" ")[1:]
    check= ['Dishwasher', 'Dryer', 'Stove', 'Fridge', 'Washer', 'Microwave', 'Air Conditioner',
           'Dehumidifier', 'Garbage Disposer', 'Freezer', 'Trash Compacter', 'Ice Maker','Refrigerator', 'Air',
           'Disposer', 'Maker']
    brand=''
    for i in uniquestr:
        if i in check:
            applianceidx= check.index(i)
            appliancetype= check[applianceidx]
            break
        else:
            brand+=i+" "
    brand= brand[:-1]
    print(brand)
    df.loc[len(df)] = ["1", model, model, app, "N/A", "N/A", brand]
    job_elements = soup.find_all("div", class_="col-6 col-sm-4 col-md-3 col-lg-2")
    if len(job_elements)<1:
        temp= soup.find_all("li", class_= "col-md-6")
        a_val= temp[0].find_all("a")[0].get("href")
        url1= f"https://www.partselect.com{a_val}"
        page1= requests.get(url1)
        soup1= BeautifulSoup(page1.content, "html.parser")
        job_elements = soup1.find_all("div", class_="col-6 col-sm-4 col-md-3 col-lg-2")
        model= a_val.split("/")[-2]
        df.iat[0,1]=model
        df.iat[0,2]=model
    links=[]
    spans=[]
    for i in range(len(job_elements)):
        a= job_elements[i].find_all("span")
        b= job_elements[i].find_all("a")[0].get('href')
        m= f"https://www.partselect.com{b}"
        links.append(str(m))
        spans.append(a[0].text)
    
    for i in range(len(links)):
        df.loc[len(df)] =["2", model+" "+spans[i], model+" "+spans[i], model+" "+spans[i], "N/A", "N/A", brand]
        url= links[i]
        pages= requests.get(url)
        soup1= BeautifulSoup(pages.content, "html.parser")
        part_ele= soup1.find_all("div", class_="three-pane__model-display__parts-list__part-item__part-info__part-name")
        ele= soup1.find_all("div", class_="three-pane__model-display__parts-list js-ua-ms-partslist")
        span= ele[0].find_all('span')
        price_find= ele[0].find_all("div", class_="three-pane__model-display__parts-list__part-item__part-price")
        part_name=[]
        psnum=[]
        mnum=[]
        availability=[]
        cost=[]
        man_name=[]
        for j in range(len(span)):
            a= span[j].find_all('a')
            if len(a)>0:
                b= a[0]['href'].split('-')
                c= a[0].text
                mnum.append(b[2])
                psnum.append(c)
                man_name.append(b[1])
                
        av= soup1.find_all("a", class_= "three-pane__model-display__parts-list__part-item__part-purchase__availability stock-text js-tooltip")

        for k in range(len(psnum)):
            a1=part_ele[k]
            name= a1.find_all('a')
            part_name.append(name[0].text)
            avail= av[k].text
            avail= avail.replace("\r", "")
            avail= avail.replace("\t", "")
            avail= avail.replace("\s", "")
            avail= avail.replace("\n\n","")
            avail= avail.replace(" ","")
            avail= avail[:-1]
            availability.append(avail)
            price= price_find[k].find_all("div", class_="price")
            if len(price)>0:
                dollar_val= price[0].find("span", class_="dollar")
                cents_val= price[0].find("span", class_="cents")
                d= str(dollar_val.text)
                c= str(cents_val.text)
                price1=d+c
                cost.append(price1)
            else:
                cost.append("Null")
            
        partlevel= ["3" for i in range(len(part_name))]
        data= {'Level':partlevel, "Manufacturer Part No.":mnum, "PS No.": psnum, "Part Name":part_name, 
                           "Availability":availability, "Price":cost, "Manufacturer name":man_name}
        df1 = pd.DataFrame(data)
        df= pd.concat([df, df1]).reset_index(drop=True)
    
    return df, brand, appliancetype


# In[7]:


def checkmodel(model):
    URL= f"https://www.partselect.com/Models/{model}/"
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    title= soup.find_all("h1", class_="title-main mt-3 mb-4")
    print("\n"+"Model you entered:", model)
    if len(title)<1:
        while len(title)<1:
            model=model[:-1]
            URL= f"https://www.partselect.com/Models/{model}/"
            page = requests.get(URL)
            soup = BeautifulSoup(page.content, "html.parser")
            title= soup.find_all("h1", class_="title-main mt-3 mb-4")
    print("Model found:", model)
    job_elements = soup.find_all("div", class_="col-6 col-sm-4 col-md-3 col-lg-2")
    if len(job_elements)<1:
        temp= soup.find_all("li", class_= "col-md-6")
        newmodels=[]
        for i in range(len(temp)):
            model_span= temp[i].find_all("span")
            if len(model_span)>1:
                newmodels.append((model_span[0].text)+" "+(model_span[2].text))
            else:
                newmodels.append(model_span[0].text)
        print("\n"+"Please narrow your search further:",)
        for (i, item) in enumerate(newmodels, start=1):
            print(i, item)
        return newmodels,False
    return [],True


# In[46]:


def new_generate_data(val):
    count=1
    m=[]
    m.append(val)
    for i in m:
        sqldf_supplier= pd.read_sql_table('supplier2', con=engine)
        sqldf_parts= pd.read_sql_table('parts2', con=engine)
        sqldf_inventory= pd.read_sql_table('inventory2', con=engine)
        sqldf_video= pd.read_sql_table('video2', con=engine)
        #sqldf_BOM= pd.read_sql_table('Bom', con=engine)
        
        unique_model= set(sqldf_parts['Parts_UID'].str.split('_').str[0].unique())
        print(unique_model)
        if i in unique_model:
            break
        df_supplier= pd.DataFrame(columns= ['Supplier_UID', 'Supplier_name', 'Supplier_website'])
        
        alt_models,model_check= checkmodel(i)
        if not model_check:
            return alt_models, False
#             updatemodel= input("Select the option number: ")
#             i= alt_models[int(updatemodel)-1]
        new, brand, appliancetype= createlevels(i)
        
        for index,row in new.iterrows():
            ps= row['PS No.']
            if row["Manufacturer Part No."]=="N":
                new.at[index, "Manufacturer Part No."]=ps
        
        un=new["Manufacturer name"].unique()       
        supps=set()
        for sentence in un:
            found = False
            for x,word in sqldf_supplier.iterrows():
                if word['Supplier_name'] in sentence:
                    found = True
                    break
            if not found:
                supps.add(sentence)

        if 'PartSelect' not in sqldf_supplier['Supplier_name'].values:
            supps.add("PartSelect")
        
        last_sup_id= get_supplier_uid()
        suplen= (int(last_sup_id[0][0][-6:]))
        if len(supps)>0:
            for j in supps:
                jweb= re.sub(r'[^a-zA-Z]+', '', j)
                df_supplier.loc[len(df_supplier)] =["400-"+str(suplen).zfill(6), j , f"www.{jweb.lower()}.com"]
                suplen+=1
        df_supplier.to_sql('supplier2', con=engine, if_exists='append', index=False)
        
        
        #SQL Parts update
        df_parts= new.copy()
        df_parts.drop(columns=['Level','PS No.', 'Availability', 'Price'], inplace=True)
        df_parts= df_parts.drop_duplicates('Manufacturer Part No.')
        df_parts.reset_index(inplace=True, drop=True)
        df_parts["Manufacturer"]="400-000000"
        
        sqldf_supplier2= pd.read_sql_table('supplier2', con=engine)
        
        for index1,row1 in df_parts.iterrows():
            for index2, row2 in sqldf_supplier2.iterrows():
                if row1['Manufacturer name']==row2["Supplier_name"]:
                    df_parts.at[index1, "Manufacturer"]= row2["Supplier_UID"]
        
        parts_dup_indices=[]
        for index1,row1 in df_parts.iterrows():
            for index2, row2 in sqldf_parts.iterrows():
                if row1['Manufacturer Part No.']==row2["Manufacturer Part No."]:
                    parts_dup_indices.append(index1)
        
        df_parts=df_parts.drop(parts_dup_indices)
        df_parts.reset_index(drop=True, inplace=True)
        
        df_parts['Parts_UID']= "200-00000"
        last_parts_uid=get_parts_uid()
        partslen= (int(last_parts_uid[0][0][-6:]))
        for j in range(partslen, len(df_parts)+partslen):
            df_parts.at[j-partslen, 'Parts_UID'] = '200-' + str(j).zfill(6)
            
        df_parts.drop(columns= ['Manufacturer name'])
        df_parts = df_parts[['Parts_UID', 'Manufacturer Part No.', 'Part Name','Manufacturer']]
        df_parts.to_sql('parts2', con=engine, if_exists='append', index=False)
        
        #SQL Inventory Update
        df_inventory= new.copy()
        df_inventory.drop(columns=['Level','Manufacturer name',"Part Name"], inplace=True)
        df_inventory= df_inventory.drop_duplicates('PS No.')
        df_inventory.reset_index(inplace=True, drop=True)
        df_inventory['Parts_UID']= "200-000001"
        df_inventory['Inventory_UID']= "500-000001"
        
        ps_value = sqldf_supplier2.loc[sqldf_supplier2['Supplier_name'] == 'PartSelect', 'Supplier_UID'].values[0]
        df_inventory['Supplier_UID']=ps_value

        inventory_dup_indices=[]
        for index1,row1 in df_inventory.iterrows():
            for index2, row2 in sqldf_inventory.iterrows():
                if row1['PS No.']==row2["PS No."]:
                    inventory_dup_indices.append(index1)
        
        df_inventory=df_inventory.drop(inventory_dup_indices)
        df_inventory.reset_index(drop=True, inplace=True)
        
        sqldf_parts2= pd.read_sql_table('parts2', con=engine)
        
        
        for index1,row1 in df_inventory.iterrows():
            for index2, row2 in sqldf_parts2.iterrows():
                if row1['Manufacturer Part No.']==row2['Manufacturer Part No.']:
                    df_inventory.at[index1, "Parts_UID"]= row2["Parts_UID"]
        df_inventory.drop(columns= ['Manufacturer Part No.'])
        
        last_inventory_uid=get_inventory_uid()
        inventorylen= (int(last_inventory_uid[0][0][-6:]))
        for j in range(inventorylen, len(df_inventory)+inventorylen):
            df_inventory.at[j-inventorylen, 'Inventory_UID'] = i+'_500-' + str(j).zfill(6)
        
        df_inventory=df_inventory[['Inventory_UID','Supplier_UID', 'Parts_UID', 'PS No.', 'Price', 'Availability']]
        df_inventory.to_sql('inventory2', con=engine, if_exists='append', index=False)
        
        #BOM update
        df_BOM= new.copy()
        df_BOM.drop(columns=['Manufacturer name',"Part Name", "PS No.", "Price", "Availability"], inplace=True)
        last_bom_uid= get_bom_uid()
        bomlen= (int(last_bom_uid[0][0][-6:]))
        df_BOM['BOM_UID']= "100-000001"
        for j in range(bomlen, len(df_BOM)+bomlen):
            df_BOM.at[j-bomlen, 'BOM_UID'] = i+'_100-' + str(j).zfill(6)
        
        df_BOM['product_PN']=None
        df_BOM['product_type']=None
        df_BOM['product_manufacturer']=None
        
        df_BOM.at[0,'product_manufacturer']=brand
        df_BOM.at[0, 'product_type']= appliancetype
        df_BOM.at[0, 'product_PN']=i
        df_BOM['Qty']= 1
        
        df_BOM=df_BOM[["BOM_UID", "Level", "Manufacturer Part No.", 'Qty', 'product_PN','product_type','product_manufacturer']]
        
        #df_BOM.to_sql('Bom', con=engine, if_exists='append', index=False)
        list_of_rows=[['BOM_UID','Level','Number', 'Qty','product_PN','product_type', 'product_manufacturer']]
        for ix, rw in df_BOM.iterrows():
            list_of_rows.append(list(rw))
        ctb_api.TraverseTable(list_of_rows,'Home Appliance')
        
        
        #Video Update
        newURL= f"https://www.partselect.com/Models/{i}/Videos"
        newpage = requests.get(newURL)
        newsoup = BeautifulSoup(newpage.content, "html.parser")
        summary= newsoup.find_all("div",class_="summary")
        if len(summary)>0:
            total_vids= (summary[0].text).split(" ")[-1]
            currvids= (summary[0].text).split(" ")[-3]
            yt_sublink= newsoup.find_all("div", class_="yt-video")
            description= newsoup.find_all("div",class_="mega-m__part mb-5")

            desc=[]
            videolinks= []
            vid_id=[]
            count1=1
            while int(currvids)<=int(total_vids):
                for j in range(len(yt_sublink)):
                    suburl= yt_sublink[j]["data-yt-init"]
                    full_link= f"https://www.youtube.com/watch?v={suburl}&ab_channel=PartSelect"
                    videolinks.append(full_link)
                    a1= description[j].find_all("a")
                    model_n= (a1[0]["title"]).split(" ")[-1]
                    desc.append(model_n)
                count1+=1
                URL1= f"https://www.partselect.com/Models/{i}/Videos/?start={count1}"
                page1 = requests.get(URL1)
                soup1 = BeautifulSoup(page1.content, "html.parser")
                summary= soup1.find_all("div",class_="summary")
                if int(currvids)==int(total_vids):
                    break
                total_vids= (summary[0].text).split(" ")[-1]
                currvids= (summary[0].text).split(" ")[-3]
                yt_sublink= soup1.find_all("div", class_="yt-video")
                description= soup1.find_all("div",class_="mega-m__part mb-5")

            last_video_uid= get_video_uid()
            videolen= (int(last_video_uid[0][0][-6:]))
            for p in range(videolen, len(desc)+videolen):
                vid_id.append(i+'_600-' + str(p).zfill(6))

            data3= {'Video_UID':vid_id,'Part_UID':desc, "Video Link":videolinks}
            df_video = pd.DataFrame(data3)

            for index1,row1 in df_video.iterrows():
                for index2, row2 in sqldf_parts2.iterrows():
                    if row1['Part_UID']==row2["Manufacturer Part No."]:
                        df_video.at[index1, "Part_UID"]= row2["Parts_UID"]
            df_video.to_sql('video2', con=engine, if_exists='append', index=False)
    return "Null", True

