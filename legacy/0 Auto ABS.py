#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import pandas as pd
import io
import requests
from datetime import date
import json
today = date.today()


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Set url

# In[3]:


exe_path = "/Users/paulkostoff/Desktop/Projects/karus abs/chromedriver"


# In[4]:


url = 'https://finsight.com/us-abs-loan-level-data-on-edgar?producs=ABS&regions=USOA'


# In[5]:


options = Options()
driver = webdriver.Chrome(executable_path = exe_path)


# In[6]:


# call url
driver.get(url)
time.sleep(3)


# In[7]:


html_source = driver.page_source
source_encoding = html_source.encode('utf-8')
soup = BeautifulSoup(source_encoding, "html.parser")


# ### Loop over pages

# In[8]:


# helper functions
def download_csv(href):
    return href and 'download-csv?fileId=' in href

def only_data_tape(text):
    return text and text.endswith('Data Tape.csv')

def get_auto(t):
    return t and t == "AUTO"


# In[9]:


page_div = soup.find('div', {'class' : '_2zNzam6jNP6zWOXPjT2YYf'})
pages = 0
if page_div:
    _, inc, total = [int(s) for s in page_div.text.split() if s.isdigit()]
    pages = int(total/inc)
    


# In[10]:


driver.get(url)
time.sleep(3)
_dict = {}
for page in range(pages):
    print('getting page {}'.format(page))
    html_source = driver.page_source
    source_encoding = html_source.encode('utf-8')
    soup = BeautifulSoup(source_encoding, "html.parser")
    for tr in soup.find_all("tr", {"class":"_35bNg_btPMMW6To403490c"}):
        val = tr.find('div', text="AUTO") # text = get_auto
        if val:
            target = tr.find_all("span", {"class":"_1z330xt6MnHtjPRyzgoymW"})
            for span in target:
                for link in span.find_all(href = download_csv, download = only_data_tape):
                    _dict[link.get('download')] = link.get('href')
    print("------------------")
    next_page_element = driver.find_elements_by_class_name("_1fxQtfNFLzsTu8kKYjDCQZ")
    if next_page_element:
        next_page_element[0].click()
        time.sleep(2)
        


# ### Export

# In[11]:


e_folder = 'data/json/'
e_file = 'all files {}.json'.format(today)
e_path = e_folder + e_file
e_path


# In[12]:


with open(e_path, 'w') as outfile:  
    json.dump(_dict, outfile, indent = 4, separators = (',', ': '), sort_keys = False)


# In[13]:


print('complete...')


# In[ ]:





# ### End
