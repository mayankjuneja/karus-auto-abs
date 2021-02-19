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

# In[6]:


load_date = '2021-01-12'


# In[7]:


folder = 'data/json/'
file = 'all files {}.json'.format(load_date)
path = folder + file
path


# In[8]:


with open(path) as f:
    files_dict = json.load(f)
    


# In[9]:


keys = list(files_dict.keys())


# In[16]:


term = '2020.12.21_GM Financial Consumer Automobile Receivables Trust'
use_files = [file for file in keys if term in file]
len(use_files)


# In[17]:


use_files


# ### Combine data tapes

# In[18]:


master_df = pd.DataFrame()
for file in use_files:
    
    print('reading {}'.format(file))
    
    url = files_dict[file]
    init = requests.get(url).content
    df = pd.read_csv(io.StringIO(init.decode('utf-8')))
    df['dataset_name'] = file
    
    master_df = master_df.append(df).reset_index(drop = True)
    


# In[19]:


master_df.shape


# In[20]:


master_df


# ### Export

# In[24]:


e_folder = 'data/combined/'
e_file = '{}.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[25]:


master_df.to_csv(e_path, index = False)


# In[26]:


print('complete...')


# In[ ]:





# ### End
