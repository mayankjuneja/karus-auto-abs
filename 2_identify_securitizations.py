#!/usr/bin/env python
# coding: utf-8

# 
# 
# ### Load libs

# In[1]:


import pandas as pd
import io
import requests
import json
import numpy as np
import time
import os
import re
from datetime import date
today = date.today()


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


folder = 'data/json/securitizations/'
files = [f for f in os.listdir(folder) if '.json' in f and '._' not in f]
files.sort()
file = files[-1]
path = folder + file


# In[4]:


file


# In[5]:


with open(path) as f:
    files_dict = json.load(f)
    


# In[6]:


keys = list(files_dict.keys())
sec_keys = list(set([k.split('_')[1] for k in keys]))


# In[7]:


sec_keys


# ### Reformat json

# In[8]:


def get_originator(sec):
    
    """
    Get originator
    """
    
    sec = sec.lower()
    if 'carmax' in sec:
        o = 'CarMax'
    elif 'volkswagen' in sec:
        o = 'Volkswagen'
    elif 'ally' in sec:
        o = 'Ally'
    elif 'hyundai' in sec:
        o = 'Hyundai'
    elif 'bmw' in sec:
        o = 'BMW'
    elif 'nissan' in sec:
        o = 'Nissan'
    elif 'americredit' in sec:
        o = 'AmeriCredit'
    elif 'honda' in sec:
        o = 'Honda'
    elif 'gm' in sec:
        o = 'GM'
    elif 'world omni' in sec:
        o = 'World_Omni'
    elif 'drive auto' in sec and 'santander' not in sec:
        o = 'Drive_Auto'
    elif 'usaa' in sec:
        o = 'USAA'
    elif 'toyota' in sec:
        o = 'Toyota'
    elif 'ford' in sec:
        o = 'Ford'
    elif 'harley-davidson' in sec:
        o = 'Harley_Davidson'
    elif 'santander' in sec:
        o = 'Santander'
    elif 'mercedes' in sec or 'mercedez' in sec:
        o = 'Mercedes'
    elif 'capital one' in sec:
        o = 'Capital_One'
    elif 'california' in sec:
        o = 'California_Republic'
    elif 'exeter' in sec:
        o = 'Exeter'
    elif 'fifth third' in sec:
        o = 'Fifth_Third'
    elif 'wells fargo' in sec:
        o = 'Wells_Fargo'
    elif 'carvana' in sec:
        o = 'Carvana'
    else:
        o = sec.split()[0].capitalize()
    
    if 'lease' in sec:
        t = 'lease'
    else:
        t = 'receivables'
    
    return o, t
    
    


# In[9]:


r_dict = {}
r_dict['total_securitizations'] = len(sec_keys)
for k in sec_keys:
    oritinator, _type = get_originator(k)
    counter = 0
    r_dict[k] = {}
    for k2 in files_dict:
        if k == k2.split('_')[1]:
            counter = counter + 1
            r_dict[k][k2] = files_dict[k2]
    r_dict[k]['releases'] = counter
    r_dict[k]['originator'] = oritinator
    r_dict[k]['type'] = _type
    


# ### Export

# In[10]:


e_folder = 'data/json/securitizations/detailed/'
e_file = 'detailed_{}.json'.format(today)
e_path = e_folder + e_file
e_path


# In[11]:


with open(e_path, 'w') as outfile:  
    json.dump(r_dict, outfile, indent = 4, separators = (',', ': '), sort_keys = False)


# In[12]:


print('complete...')


# In[ ]:





# ### End
