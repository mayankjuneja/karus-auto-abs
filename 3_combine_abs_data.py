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
from project_functions import get_originator


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


term = 'Toyota Auto Receivables 2020-D Owner Trust Data Tape'
securitization = [file for file in use_keys if term in file][0]
securitization


# In[6]:


sub_dict = files_dict[securitization]


# In[7]:


s_keys = list(sub_dict.keys())
s_keys = [k for k in s_keys if term in k]


# In[8]:


use_len = len(s_keys)
use_len


# In[9]:


s_keys


# In[10]:


finder = re.compile(r'\b\d{4}-[A-Za-z\d]+\b')
securitization = re.search(finder, term)
sec_use = securitization.group()
originator = get_originator(term)[0]


# In[11]:


print(originator)
print(sec_use)


# ### Combine data tapes

# In[12]:


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

# In[13]:


master_df = pd.concat(master_list)


# In[14]:


master_df = master_df.reset_index(drop = True)


# In[15]:


master_df['obligorCreditScore'].replace('None', np.nan, inplace = True)
master_df['obligorCreditScore'].replace('-', np.nan, inplace = True)
master_df['obligorCreditScore'] = master_df['obligorCreditScore'].astype(float)
master_df['obligorCreditScore'].mean()


# In[16]:


master_df.shape


# ### Export

# In[29]:


top_level = 'data/karus_datasets/{}/'.format(originator)
top_bool = os.path.isdir(top_level)
if top_bool == False:
    os.mkdir(top_level)


# In[30]:


e_folder = 'data/karus_datasets/{}/{} {}/'.format(originator, originator, sec_use)
e_folder


# In[31]:


try:
    os.mkdir(e_folder)
    print('created {}'.format(e_folder))
except:
    print('dir already exists')


# In[32]:


e_file = 'transaction_raw.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[33]:


master_df.to_csv(e_path, index = False)


# In[34]:


print('complete...')


# In[ ]:





# ### End
