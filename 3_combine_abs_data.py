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


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


# detailed json
folder = 'data/json/securitizations/detailed/'
files = [f for f in os.listdir(folder) if '.json' in f and '._' not in f]
files.sort()
file = files[-1]
path = folder + file
with open(path) as f:
    files_dict = json.load(f)
keys = list(files_dict.keys())
use_keys = [k for k in keys if k != 'total_securitizations']
use_keys.sort()
len(use_keys)


# In[4]:


use_keys


# ### Grab data

# In[5]:


term = 'Santander Drive Auto Receivables Trust 2020-4 Data Tape'
securitization = [file for file in use_keys if term in file][0]
securitization


# In[6]:


sub_dict = files_dict[securitization]


# In[7]:


s_keys = list(sub_dict.keys())
s_keys = [k for k in s_keys if k != 'releases']


# In[8]:


use_len = len(s_keys)
use_len


# ### Combine data tapes

# In[9]:


collected = []
broke = []
master_list = []
for file in s_keys:
    print('reading {}'.format(file))
    collected.append(file)
    col_len = len(collected)
    url = sub_dict[file]
    print(url)
    try:
        init = requests.get(url).content
        df = pd.read_csv(io.StringIO(init.decode('utf-8')))
        df['dataset_name'] = file.split('.csv')[0]
        master_list.append(df)
        print(round(col_len / use_len, 3))
    except:
        broke.append(file)
        print('ERROR!!!!! {}'.format(file))
    time.sleep(2)
    print('-------------------------------------')
    


# ### Quick processing

# In[10]:


master_df = pd.DataFrame()
count = 0
for df in master_list:
    count = count + 1
    print(count)
    master_df = master_df.append(df)
    


# In[11]:


master_df = master_df.reset_index(drop = True)


# In[12]:


master_df['obligorCreditScore'].replace('None', np.nan, inplace = True)
master_df['obligorCreditScore'].replace('-', np.nan, inplace = True)
master_df['obligorCreditScore'] = master_df['obligorCreditScore'].astype(float)
master_df['obligorCreditScore'].mean()


# In[13]:


master_df.shape


# ### Export

# In[14]:


e_folder = 'data/transaction/'
e_file = '{}.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[15]:


master_df.to_csv(e_path, index = False)


# In[16]:


print('complete...')


# In[ ]:





# ### End
