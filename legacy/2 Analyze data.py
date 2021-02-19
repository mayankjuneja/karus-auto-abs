#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[6]:


import pandas as pd


# In[7]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[14]:


term = '2020.12.21_GM Financial Consumer Automobile Receivables Trust'


# In[15]:


folder = 'data/combined/'
file = '{}.csv'.format(term)
path = folder + file
path


# In[16]:


data = pd.read_csv(path)


# In[17]:


data.shape


# ### Formatting

# In[22]:


def convert_id(row, column):
    
    """
    Convert ids
    """
    
    init = str(row[column])
    
    cleaned = init.replace('=', '').replace('"', '').strip()
    
    return cleaned
    


# In[23]:


data['ID'] = data.apply(convert_id, args = ('assetNumber', ), axis = 1)


# ### Quick check

# In[22]:


sub = data[data['ID'] == '845608910310916']


# In[23]:


sub


# In[7]:


data.head(1)


# In[8]:


data['chargedoffPrincipalAmount'] = data['chargedoffPrincipalAmount'].astype(float)
#data = data[data['chargedoffPrincipalAmount'] > 0]


# In[10]:


sum(data['chargedoffPrincipalAmount'])


# In[11]:


col = 'assetNumber'


# In[12]:


data[col].value_counts(dropna = False)


# In[13]:


len(data[col].unique())


# In[13]:


data


# In[ ]:





# In[ ]:





# In[20]:


print('continue...')


# In[ ]:





# In[ ]:





# ### End
