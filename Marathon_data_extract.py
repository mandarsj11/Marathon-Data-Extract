import pandas as pd
import sqlite3 #Database library
import os #OS related activities
from urllib.request import urlopen, urlretrieve
import requests #get url
from bs4 import BeautifulSoup #for web scrapping
from urllib.error import HTTPError

con = sqlite3.connect('Marathon_data.db')
cursorObj = con.cursor()
df_event_info = pd.DataFrame()
""" SQLite tutorials
https://likegeeks.com/python-sqlite3-tutorial/
""" 
count = 76
while count < 192:
    # Stage 1: Get event info from website
    url_info = 'https://www.alpharacingsolution.com/result/display/'+ str(count)
    try:
        web_data = requests.get(url_info)
        soup = BeautifulSoup(web_data.content, 'html.parser')
        table = soup.find('div', {'class':'container'})      
        dict_event_info = {'EventCount':count,
                           'EventCity': table.h4.text[:table.h4.text.index(',')],
                           'EventName': table.h3.text,
                           'EventDate': ''.join(table.h4.text[table.h4.text.index(',')+2:]) + ' 2017',
                           'EventTimerCompany': 'Alpha Racing Solution',
                           }
    
        df_event_info = df_event_info.append({'EventCount':count,
                                             'EventCity': table.h4.text[:table.h4.text.index(',')],
                                             'EventName': table.h3.text,
                                             'EventDate': ''.join(table.h4.text[table.h4.text.index(',')+2:]) + ' 2017',
                                             'EventTimerCompany': 'Alpha Racing Solution',
                                             #'recordCount': len(marathon_event)
                                             },ignore_index=True)
    except requests.exceptions.HTTPError as err:
        print('error in display - ',err, 'for EventCount - ',count)
    except AttributeError as err1:
        print('error in display - ',err1, 'for EventCount - ',count)       
    # Stage 2: download csv from website
    url = 'https://www.alpharacingsolution.com/result/download/'+ str(count)
    OUTPUT_DIR = ''
    filename = os.path.join(OUTPUT_DIR, url.rsplit('/', 1)[-1])
    try:
        urlretrieve(url, filename)
        os.rename(filename, filename + ".csv")
    
        # download csv from url : https://stackoverflow.com/questions/34632838/download-xls-files-from-a-webpage-using-python-and-beautifulsoup
        marathon_event = pd.read_csv(filename +'.csv') 
        print('No of records for EventCount - ',count, ' is - ',len(marathon_event) )
        # Stage 3: Writing DF into sqlite
        marathon_event = marathon_event.assign(**dict_event_info)
        marathon_event.to_sql('marathon_event_data', con, if_exists='append', index=False)
        con.commit() 
    except HTTPError as errors:
        print('error in download - ',errors.code, 'for EventCount - ',count)
        count = count + 1
        continue
    count = count + 1
