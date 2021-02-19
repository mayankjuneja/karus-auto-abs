#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
import io
import requests
import json


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


load_date = '2021-01-12'


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


# In[10]:


term = 'CarMax Auto Owner Trust 2017-1 Data Tape'
use_files = [file for file in keys if term in file]
len(use_files)


# In[11]:


use_len = len(use_files)
use_files


# ### Combine data tapes

# In[12]:


collected = []
master_df = pd.DataFrame()
for file in use_files:
    
    try:
    
        print('reading {}'.format(file))
        collected.append(file)
        col_len = len(collected)

        url = files_dict[file]
        init = requests.get(url).content
        df = pd.read_csv(io.StringIO(init.decode('utf-8')))
        df['dataset_name'] = file

        master_df = master_df.append(df).reset_index(drop = True)

        print(round(col_len / use_len, 3))
        
    except:
        print("can't download {}".format(file))
        
    print('-------------------------------------')
    


# In[13]:


master_df.shape


# In[14]:


master_df


# ### Export

# In[15]:


e_folder = 'data/combined/'
e_file = '{}.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[16]:


master_df.to_csv(e_path, index = False)


# In[17]:


print('complete...')


# In[ ]:





# ### End
