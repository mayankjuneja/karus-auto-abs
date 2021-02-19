#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
import json
import numpy as np
import random
from pandarallel import pandarallel
import re
import os

pandarallel.initialize()


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


term = 'AmeriCredit Automobile Receivables Trust 2017-1 Data Tape'


# In[4]:


# load abs
folder = 'data/transaction/'
file = '{}.csv'.format(term)
path = folder + file
data = pd.read_csv(path)
data.shape


# In[5]:


# load fields
f_folder = 'data/json/fields/'
f_file = 'fields.json'
f_path = f_folder + f_file
with open(f_path) as f:
    fields = json.load(f)
    


# In[6]:


# load mapper
m_folder = 'data/dictionary/mapper/'
m_file = 'mapper.json'
m_path = m_folder + m_file
with open(m_path) as f:
    mapper = json.load(f)
    


# In[7]:


# find securitization
finder = re.compile('\d{4,}\W\d{1,}')
add_id = re.findall(finder, term)[0]
add_id


# In[8]:


# find file
s_folder = 'data/static/'
s_file = [f for f in os.listdir(s_folder) if term in f and '._' not in f][0]
s_path = s_folder + s_file
s_path


# In[9]:


static = pd.read_csv(s_path)


# ### Formatting

# In[10]:


init_id = fields['init_id'][0]
date_cols = fields['dates']
replacer_cols = fields['replace_dash']
clean_cols = fields['clean']
m_cols = fields['map']


# In[11]:


def reorder_date(init):
    
    """
    Reorder date
    """
    
    init = str(init)
    if init != '-':
        if '/' not in init:
            y = init[6:10]
            m = init[0:2]
            d = init[3:5]
            date = y + '-' + m + '-' + d
        elif '/' in init:
            y = init[3:7]
            m = init[0:2]
            date = y + '-' + m
    else:
        date = ''
    
    return date
    


# In[12]:


data['id'] = data[init_id].str.replace('=', '').str.replace('"', '').str.strip() + '-' + add_id


# In[13]:


for col in date_cols:
    print(col)
    values = data[col].values
    dates = [reorder_date(v) for v in values]
    data['{}R'.format(col)] = dates
    


# ### Replacing values

# In[14]:


data[replacer_cols] = data[replacer_cols].replace('-', np.nan)


# In[15]:


# clean cols
for col in clean_cols:
    data[col] = data[col].str.strip()
    data[col] = data[col].astype(float)
    


# In[16]:


def replace_val(init, column):
    
    """
    Replace numeric values
    """
    
    init = str(init).strip().replace(';', '')
    if init in ['0', '1', '2', '3', '4', '5', '98', '99']:
        mapped = mapper[column][init]
        return mapped
    else:
        if init[0] in ['0', '1', '2', '3', '4', '5']:
            use = init[0]
        elif init == '-':
            use_keys = list(mapper[column].keys())
            if '98' in use_keys:
                use = '98'
            elif '99' in use_keys:
                use = '99'
        else:
            use = init
        mapped = mapper[column][use]
        
    return mapped
    


# In[17]:


for col in m_cols:
    print(col)
    values = data[col].values
    ret_vals = [replace_val(v, col) for v in values]
    data['{}M'.format(col)] = ret_vals
    


# ### Account status

# In[18]:


def acct_status(row, b_col, e_col, zero_col, thresh):
    
    """
    Create karus account status
    """
    
    b = float(row[b_col])
    e = float(row[e_col])
    z = str(row[zero_col])
    
    if z in ['Charged-off', 'Repurchased or Replaced']:
        res = z
        return res
    if b < thresh and e < thresh:
        res = 'Prepaid or Matured'
        return res
    if z in ['Unavailable', 'Prepaid or Matured']:
        res = z
        return res
    


# In[19]:


b_col = 'reportingPeriodBeginningLoanBalanceAmount'
e_col = 'nextReportingPeriodPaymentAmountDue'
z_col = 'zeroBalanceCodeM'
thresh = 50


# In[20]:


data['accountStatus'] = data.parallel_apply(acct_status, args = (b_col, e_col, z_col, thresh, ), axis = 1)


# In[21]:


data['accountStatus'].value_counts()


# ### Add target

# In[22]:


m_col = 'id'
m_type = 'left'
merged = pd.merge(data, static[[m_col, 'target']], on = m_col, how = m_type)
merged.shape


# In[23]:


merged['target'].value_counts(dropna = False)


# ### Export

# In[24]:


e_folder = 'data/transaction/prepared/'
e_file = '{} transaction prepared.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[25]:


merged.to_csv(e_path, index = False)


# In[26]:


print('continue...')


# In[ ]:





# ### End
