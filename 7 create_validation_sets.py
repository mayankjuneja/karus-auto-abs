#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
import re
import os


# ### Load data

# In[2]:


term = 'AmeriCredit Automobile Receivables Trust 2020-3 Data Tape'


# In[3]:


finder = re.compile('\d{4,}\W\d{1,}')
add_id = re.findall(finder, term)[0]
add_id


# In[4]:


# static
s_folder = 'data/static/'
s_file = '{} static.csv'.format(term)
s_path = s_folder + s_file
static = pd.read_csv(s_path)


# In[5]:


# transaction
t_folder = 'data/transaction/prepared/'
t_file = '{} transaction.csv'.format(term)
t_path = t_folder + t_file
transaction = pd.read_csv(t_path)


# ### Make directory

# In[ ]:


r_folder = 'data/validation/{}/'.format(term)
try:
    os.mkdir(r_folder)
    print('created {}'.format(r_folder))
except:
    print('already exists')


# ### Create subset

# In[ ]:


all_targets = list(static['target'].unique())


# In[ ]:


static['target'].value_counts()


# In[ ]:


dfs_dict = {}
for t in all_targets:
    
    print(t)
    
    s_sub = static[static['target'] == t]
    s_ran = s_sub.sample(3)
    _ids = list(s_ran['id'].unique())
    t_ran = transaction[transaction['id'].isin(_ids)]
    
    s_path = r_folder + '{} static.xlsx'.format(t)
    t_path = r_folder + '{} transaction.xlsx'.format(t)
    s_ran.to_excel(s_path)
    t_ran.to_excel(t_path)
    


# In[ ]:


print('complete...')


# In[ ]:





# ### End
