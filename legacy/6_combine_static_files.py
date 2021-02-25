#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
import os
import re
from pandarallel import pandarallel
pandarallel.initialize()


# In[2]:


n = 750
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Combine files

# In[3]:


term = 'AmeriCredit Automobile Receivables Trust'


# In[4]:


folder = 'data/static/'
files = [folder + file for file in os.listdir(folder) if '.csv' in file and term in file and '._' not in file]


# In[5]:


files.sort()
files


# In[6]:


finder = re.compile('\d{4,}\W\d{1,}')


# In[7]:


master = pd.DataFrame()
for file in files:
    #add_id = re.findall(finder, file)[0]
    df = pd.read_csv(file)
    #df['securitization'] = add_id
    print(df.shape)
    master = master.append(df).reset_index(drop = True)


# In[8]:


master.shape


# In[9]:


master['target'].value_counts(dropna = False)


# ### Fix vals

# In[10]:


vals_cols = [col for col in list(master.columns) if 'vals' in col.lower()]


# In[11]:


def fix_vals(row, column):

    """
    Fix column values
    """

    init = str(row[column])

    ret_val = 'str: ' + init

    return ret_val


# In[12]:


for col in vals_cols:
    print(col)
    master[col] = master[col].astype(str)
    master[col] = 'str: ' + master[col]
    


# In[13]:


master.shape


# ### Export

# In[14]:


e_folder = folder + 'combined/'
e_file = '{} securitizations.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[15]:


master.to_csv(e_path, index = False)


# In[16]:


print('complete...')


# In[ ]:





# ### End
