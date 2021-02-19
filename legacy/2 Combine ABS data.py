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


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


load_date = '2021-02-02'


# In[4]:


folder = 'data/json/'
file = 'all files {}.json'.format(load_date)
path = folder + file
path


# In[5]:


with open(path) as f:
    files_dict = json.load(f)
    


# In[6]:


keys = list(files_dict.keys())


# In[7]:


keys


# In[26]:


term = 'USAA Auto Owner Trust 2019-1 Data Tape'
use_files = [file for file in keys if term in file]
len(use_files)


# In[27]:


use_len = len(use_files)
use_files


# ### Combine data tapes

# In[28]:


collected = []
master_df = pd.DataFrame()
for file in use_files:
    
    print('reading {}'.format(file))
    try:
        collected.append(file)
        col_len = len(collected)
        url = files_dict[file]
        print(url)
        init = requests.get(url).content
        df = pd.read_csv(io.StringIO(init.decode('utf-8')))
        df['dataset_name'] = file
        master_df = master_df.append(df).reset_index(drop = True)
        print(round(col_len / use_len, 3))
        print(master_df[''])
        time.sleep(2)
    except:
        print('ERROR {}'.format(file))
        time.sleep(30)

    print('-------------------------------------')
    


# In[29]:


master_df.shape


# In[30]:


master_df['obligorCreditScore'].replace('None', np.nan, inplace = True)
master_df['obligorCreditScore'] = master_df['obligorCreditScore'].astype(float)
master_df['obligorCreditScore'].mean()


# ### Export

# In[31]:


e_folder = 'data/combined/'
e_file = '{}.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[32]:


master_df.to_csv(e_path, index = False)


# In[33]:


print('complete...')


# In[ ]:





# ### End
