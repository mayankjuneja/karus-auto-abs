#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[11]:


import pandas as pd
import numpy as np


# ### Load data

# In[15]:


term = 'Fifth Third Auto Trust 2017-1'


# In[16]:


folder = 'data/combined/'
file = '{}.csv'.format(term)
path = folder + file
data = pd.read_csv(path)
data.shape


# In[17]:


data['obligorCreditScore'].replace('None', np.nan, inplace = True)
data['obligorCreditScore'].replace('NONE', np.nan, inplace = True)
data['obligorCreditScore'] = data['obligorCreditScore'].astype(float)
data['obligorCreditScore'].mean()


# In[18]:


print('complete...')


# In[ ]:





# ### End
