

from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import json
import re
import requests

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
            break
        else:
            brand+=i+" "
    brand= brand[:-1]
    print(brand)
    df.loc[len(df)] = [1, model, model, app, "N/A", "N/A", brand]
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
        df.loc[len(df)] =[2, model+" "+spans[i], model+" "+spans[i], model+" "+spans[i], "N/A", "N/A", brand]
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
            
        partlevel= [3 for i in range(len(part_name))]
        data= {'Level':partlevel, "Manufacturer Part No.":mnum, "PS No.": psnum, "Part Name":part_name, 
                           "Availability":availability, "Price":cost, "Manufacturer name":man_name}
        df1 = pd.DataFrame(data)
        df= pd.concat([df, df1]).reset_index(drop=True)
    
    return df

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

m= ['KFCS22EVBL3','WRT519SZDM0']


def generate_data(m):
    
    
    df_supplier= pd.DataFrame(columns= ['Supplier_UID', 'Supplier_name', 'Supplier_website'])
    df_supplier.loc[len(df_supplier)]= ['400-000001', 'PartSelect', 'www.partselect.com']
    
    count=1
    for i in m:
        alt_models,model_check= checkmodel(i)
        if not model_check:
            updatemodel= input("Select the option number: ")
            i= alt_models[int(updatemodel)-1]
        new= createlevels(i)
        
        for index,row in new.iterrows():
            ps= row['PS No.']
            if row["Manufacturer Part No."]=="N":
                new.at[index, "Manufacturer Part No."]=ps
                
        #Supplier Data
        un=new["Manufacturer name"].unique()       
        supps=set()
        for sentence in un:
            found = False
            for x,word in df_supplier.iterrows():
                if word['Supplier_name'] in sentence:
                    found = True
                    break
            if not found:
                supps.add(sentence)
    
        if len(supps)>0:
            for j in supps:
                suplen=len(df_supplier)
                jweb= re.sub(r'[^a-zA-Z]+', '', j)
                df_supplier.loc[len(df_supplier)] =["400-"+str(suplen+1).zfill(6), j , f"www.{jweb.lower()}.com"]
        
        
        #Parts Update
        df_parts= new.copy()
        df_parts.drop(columns=['Level','PS No.', 'Availability', 'Price'], inplace=True)
        df_parts= df_parts.drop_duplicates('Manufacturer Part No.')
        df_parts.reset_index(inplace=True, drop=True)
        df_parts["Manufacturer"]="400-000000"
            
        for index1,row1 in df_parts.iterrows():
            for index2, row2 in df_supplier.iterrows():
                if row1['Manufacturer name']==row2["Supplier_name"]:
                    df_parts.at[index1, "Manufacturer"]= row2["Supplier_UID"]
            
        df_parts['Parts_UID']= "200-00000"
        for j in range(len(df_parts)):
            df_parts.at[j, 'Parts_UID'] = i+' 200-' + str(j+1).zfill(6)
        
        df_parts.drop(columns= ['Manufacturer name'])
        
        #Inventory Update
        df_inventory= new.copy()
        df_inventory.drop(columns=['Level','Manufacturer Part No.','Manufacturer name',"Part Name"], inplace=True)
        df_inventory= df_inventory.drop_duplicates('PS No.')
        df_inventory.reset_index(inplace=True, drop=True)
        df_inventory['Parts_UID']= "200-000001"
        df_inventory['Inventory_UID']= "500-000001"
        df_inventory['Supplier_UID']="400-000001"
        
        for j in range(len(df_inventory)):
            df_inventory.at[j, 'Parts_UID'] = i+' 200-' + str(j+1).zfill(6)

        for j in range(len(df_inventory)):
            df_inventory.at[j, 'Inventory_UID'] = i+' 500-' + str(j+1).zfill(6)
            
        
        #BOM Update
        df_BOM= new.copy()
        df_BOM.drop(columns=['Manufacturer name',"Part Name", "PS No.", "Price", "Availability"], inplace=True)
        df_BOM['BOM_UID']= "100-000001"
        
        for j in range(len(df_BOM)):
            df_BOM.at[j, 'BOM_UID'] = i+' 100-' + str(j+1).zfill(6)
        
        df_BOM['Qty']= 1
        
        df_BOM=df_BOM[["BOM_UID", "Level", "Manufacturer Part No."]]
        df_parts = df_parts[['Parts_UID', 'Manufacturer Part No.', 'Part Name','Manufacturer']]
        df_inventory=df_inventory[['Inventory_UID','Supplier_UID', 'Parts_UID', 'PS No.', 'Price', 'Availability']]
        
        df_BOM.to_csv(f'df_BOM{count}.csv', index=False)
        df_parts.to_csv(f'df_parts{count}.csv', index=False)
        df_inventory.to_csv(f'df_inventory{count}.csv', index=False)
        
        
        
        #Video update
        newURL= f"https://www.partselect.com/Models/{i}/Videos"
        newpage = requests.get(newURL)
        newsoup = BeautifulSoup(newpage.content, "html.parser")
        summary= newsoup.find_all("div",class_="summary")
        total_vids= (summary[0].text).split(" ")[-1]
        currvids= (summary[0].text).split(" ")[-3]
        yt_sublink= newsoup.find_all("div", class_="yt-video")
        description= newsoup.find_all("div",class_="mega-m__part mb-5")
        
        desc=[]
        videolinks= []
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
        
        data3= {'Part_UID':desc, "Video Link":videolinks}
        df_video = pd.DataFrame(data3)
        
        for index1,row1 in df_video.iterrows():
            for index2, row2 in df_parts.iterrows():
                if row1['Part_UID']==row2["Manufacturer Part No."]:
                    df_video.at[index1, "Part_UID"]= row2["Parts_UID"]
        df_video.to_csv(f'df_video{count}.csv', index=False)
        count+=1
    df_supplier.to_csv('df_supplier.csv', index=False)
    
generate_data(m)

