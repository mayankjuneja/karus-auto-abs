#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


term = 'AmeriCredit Automobile Receivables Trust 2019-1 Data Tape'


# In[4]:


folder = 'data/static/'
file = '{} static.csv'.format(term)
path = folder + file
path


# In[5]:


data = pd.read_csv(path)


# ### Engineering

# In[6]:


def get_target(row):
    
    """
    Set target var
    """
    
    init = str(row['accountStatusEvent'])
    remaining = row['remainingTermToMaturityNumberMinPrior']
    
    if init == 'Charged-off':
        res = 'Charged-off'
        return res

    if init == 'Prepaid or Matured' and remaining > 0:
        res = 'Prepaid'
        return res
    
    if init == 'Prepaid or Matured' and remaining < 1:
        res = 'Closed'
        return res
    
    if init == 'nan':
        res = 'Active or other'
        return res
    


# In[7]:


data['target'] = data.apply(get_target, axis = 1)


# In[8]:


data['target'].value_counts()


# In[9]:


e_folder = folder + 'prepared/'
e_file = '{} static prepared.csv'.format(term)
e_path = e_folder + e_file


# In[10]:


data.to_csv(e_path, index = False)


# In[11]:


print('complete...')


# In[ ]:





# ### End
