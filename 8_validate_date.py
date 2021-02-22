#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[16]:


import pandas as pd
import random
import os


# ### Load files

# In[25]:


folder = 'data/transaction/'
files = [f for f in os.listdir(folder ) if '._' not in f and '.DS' not in f and 'prepared' not in f]


# In[26]:


files


# ### Run results

# In[30]:


holder = []
for f in files:
    print(f)
    data = pd.read_csv(folder + f)
    init = data.shape[0]
    prep = data.drop_duplicates(subset = ['assetNumber', 'reportingPeriodBeginningDate'], keep = 'first')
    sec = prep.shape[0]
    
    _bool = init == sec
    
    res = {}
    res['file'] = f
    res['init'] = init
    res['sec'] = sec
    res['bool'] = _bool
    holder.append(res)
    


# In[31]:


df = pd.DataFrame(holder)


# In[32]:


df


# In[33]:


print('continue...')


# In[ ]:





# ### End
