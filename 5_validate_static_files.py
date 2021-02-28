#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd


# In[13]:


n = 750
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


originator = 'Santander'
add_id = '2020-4'


# In[6]:


folder = 'data/karus_datasets/{}/{} {}/'.format(originator, originator, add_id)
file = '{} static.csv'.format(add_id)
path = folder + file
path


# In[7]:


data = pd.read_csv(path)


# ### Validate subset

# In[10]:


subset = data[(data['target'].isin(['Charged-off', 'Prepaid', 'Closed'])) & (data['exceptionStatus'].isin([True]))].reset_index(drop = True)     


# In[12]:


subset['target'].value_counts()


# In[15]:


print('continue...')


# In[ ]:





# ### End
