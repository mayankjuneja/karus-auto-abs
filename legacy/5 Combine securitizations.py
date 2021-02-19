#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
import os
import re


# ### Combine files

# In[2]:


term = 'AmeriCredit Automobile Receivables Trust'


# In[3]:


folder = 'data/static/prepared/'
files = [folder + file for file in os.listdir(folder) if '.csv' in file and term in file]


# In[4]:


files.sort()
files


# In[5]:


finder = re.compile('\d{4,}\W\d{1,}')


# In[6]:


master = pd.DataFrame()
for file in files:
    add_id = re.findall(finder, file)[0]
    df = pd.read_csv(file)
    df['securitization'] = add_id
    print(df.shape)
    master = master.append(df).reset_index(drop = True)


# In[7]:


master.shape


# ### Export

# In[8]:


e_folder = folder + 'combined/'
e_file = '{} securitizations.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[9]:


master.to_csv(e_path, index = False)


# In[10]:


print('complete...')


# In[ ]:





# ### End
