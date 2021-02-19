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
import time
import multiprocessing
from multiprocessing import Pool
from joblib import Parallel, delayed
import sys

pandarallel.initialize()


# In[2]:


n = 750
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


term = 'AmeriCredit Automobile Receivables Trust 2017-1 Data Tape'
finder = re.compile('\d{4,}\W\d{1,}')
add_id = re.findall(finder, term)[0]
add_id


# In[4]:


# load abs
folder = 'data/transaction/'
file = '{}.csv'.format(term)
path = folder + file
data = pd.read_csv(path)
data.shape


# In[5]:


data = data.drop_duplicates()
data.shape


# In[6]:


# load fields
f_folder = 'data/json/fields/'
f_file = 'fields.json'
f_path = f_folder + f_file
with open(f_path) as f:
    fields = json.load(f)


# In[7]:


# load mapper
m_folder = 'data/dictionary/mapper/'
m_file = 'mapper.json'
m_path = m_folder + m_file
with open(m_path) as f:
    mapper = json.load(f)
    


# ### Setting fields

# In[8]:


init_id = fields['init_id'][0]
date_cols = fields['dates']
replacer_cols = fields['replace_dash']
clean_cols = fields['clean']
m_cols = fields['map']
event_cols = fields['event']
loc_cols = fields['all_loc']
numeric_cols = fields['numeric']
all_vals_cols = fields['all_vals']
min_max_cols = fields['min_max']


# ### ID and dates

# In[9]:


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


# In[10]:


data['id'] = data[init_id].str.replace('=', '').str.replace('"', '').str.strip() + '-' + add_id


# In[11]:


for col in date_cols:
    print(col)
    values = data[col].values
    dates = [reorder_date(v) for v in values]
    data['{}R'.format(col)] = dates
    


# In[12]:


# s_col = 'loanMaturityDate'
# t_col = '{}R'.format(s_col)
# data[[s_col, t_col]]


# ### Replacing values

# In[13]:


data[replacer_cols] = data[replacer_cols].replace('-', np.nan)


# In[14]:


# clean cols
for col in clean_cols:
    data[col] = data[col].str.strip()
    data[col] = data[col].astype(float)
    


# ### Replacing values

# In[15]:


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
    


# In[16]:


for col in m_cols:
    print(col)
    values = data[col].values
    ret_vals = [replace_val(v, col) for v in values]
    data['{}M'.format(col)] = ret_vals
    


# In[17]:


# s_col = 'subvented'
# t_col = '{}M'.format(s_col)
# data[[s_col, t_col]]


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


# ### Numeric conversion

# In[22]:


# force convert cols to numeric
for col in numeric_cols:
    print(col)
    data[col] = pd.to_numeric(data[col], errors='coerce')
    


# ### Application

# In[23]:


id_col = 'id'
status_col = 'accountStatus'
values = ['Charged-off', 'Prepaid or Matured', 'Repurchased or Replaced']


# In[24]:


all_ids = list(data[id_col].unique())
#all_ids = all_ids[:1000]
print_vals = list(range(0, len(all_ids), 100))
len(all_ids)


# In[25]:


# break ids into list chunks
num = 1000
id_lists = [all_ids[i:i + num] for i in range(0, len(all_ids), num)]  
#id_lists = [id_lists[0]]


# In[26]:


def convert_static(df):
    
    """
    Create static df
    """
    
    #df = init.reset_values(drop = True)
    df.reset_index(drop = True, inplace = True)
    df = df.sort_values('reportingPeriodBeginningDateR', ascending = False)
    df['indexAccount'] = df.index
    _id = df[id_col].iloc[0]

    # dict
    account_dict = {}
    account_dict[id_col] = _id
    account_dict['records'] = len(df)

    # current status of loan
    for col in loc_cols:
        account_dict['{}LocCurrent'.format(col)] = df[col].iloc[0]
    for col in min_max_cols:
        account_dict['{}MaxCurrent'.format(col)] = df[col].max()
        account_dict['{}MinCurrent'.format(col)] = df[col].min()
    for col in all_vals_cols:
        vals = list(df[col].unique())
        use_vals = ' | '.join(str(val) for val in vals)
        account_dict['{}ValsCurrent'.format(col)] = use_vals
    for col in numeric_cols:
        _sum = df[col].sum()
        account_dict['{}SumCurrent'.format(col)] = _sum
        vec = list(df[col])
        vec = [v for v in vec if str(v) != 'nan']
        if len(vec) > 0:
            _len = len(vec)
            weights = sorted([1 + i for i in list(range(_len))], reverse=True)
            wa = np.average(vec, weights=weights)
            account_dict['{}WeightedCurrent'.format(col)] = wa
        else:
            account_dict['{}WeightedCurrent'.format(col)] = 0

    # event information
    init_vals = list(df[status_col].unique())
    inter = list(set(values).intersection(init_vals))
    if len(inter) > 0:
        account_dict['eventOccurred'] = 1
        n = df[status_col].where(df[status_col].isin(values)).last_valid_index()
        account_dict['eventIndex'] = n
        n_bool = True
        single = df.loc[[n]]
        for col in event_cols:
            account_dict['{}Event'.format(col)] = single[col].iloc[0]

        # prior to event
        init = n+1
        sub = df[init:len(df)]
        sub.reset_index(drop = True, inplace = True)
        account_dict['priorHistory'] = len(sub)
        sub_bool = True
        if len(sub) > 0:
            for col in loc_cols:
                account_dict['{}LocPrior'.format(col)] = sub[col].iloc[0]
            for col in min_max_cols:
                account_dict['{}MinPrior'.format(col)] = sub[col].min()
                account_dict['{}MaxPrior'.format(col)] = sub[col].max()
            for col in all_vals_cols:
                vals = list(sub[col].unique())
                use_vals = ' | '.join(str(val) for val in vals)
                account_dict['{}ValsPrior'.format(col)] = use_vals
            for col in numeric_cols:
                account_dict['{}SumPrior'.format(col)] = sub[col].sum()
                vec = list(sub[col])
                vec = [v for v in vec if str(v) != 'nan']
                if len(vec) > 0:
                    _len = len(vec)
                    weights = sorted([1 + i for i in list(range(_len))], reverse=True)
                    wa = np.average(vec, weights=weights)
                    account_dict['{}WeightedPrior'.format(col)] = wa
                else:
                    account_dict['{}WeightedPrior'.format(col)] = 0

            # random
            len_sub = len(sub)            
            s = random.randint(0, len_sub)
            if s == len_sub:
                s = s -1
            r_sub = sub[s:len_sub].reset_index(drop = True)
            account_dict['randomIndex'] = s
            for col in loc_cols:
                account_dict['{}LocRandom'.format(col)] = r_sub[col].iloc[0]
            for col in min_max_cols:
                account_dict['{}MinRandom'.format(col)] = r_sub[col].min()
                account_dict['{}MaxRandom'.format(col)] = r_sub[col].max()
            for col in all_vals_cols:
                vals = list(r_sub[col].unique())
                use_vals = ' | '.join(str(val) for val in vals)
                account_dict['{}ValsRandom'.format(col)] = use_vals
            for col in numeric_cols:
                account_dict['{}SumRandom'.format(col)] = r_sub[col].sum()
                vec = list(r_sub[col])
                vec = [v for v in vec if str(v) != 'nan']
                if len(vec) > 0:
                    _len = len(vec)
                    weights = sorted([1 + i for i in list(range(_len))], reverse=True)
                    wa = np.average(vec, weights=weights)
                    account_dict['{}WeightedRandom'.format(col)] = wa
                else:
                    account_dict['{}WeightedRandom'.format(col)] = 0

        # if event is first row of sub       
        else:
            for col in loc_cols:
                account_dict['{}LocPrior'.format(col)] = df[col].iloc[0]
            for col in min_max_cols:
                account_dict['{}MinPrior'.format(col)] = df[col].min()
                account_dict['{}MaxPrior'.format(col)] = df[col].max()
            for col in all_vals_cols:
                vals = list(df[col].unique())
                use_vals = ' | '.join(str(val) for val in vals)
                account_dict['{}ValsPrior'.format(col)] = use_vals
            for col in numeric_cols:
                account_dict['{}SumPrior'.format(col)] = df[col].sum()
                account_dict['{}WeightedPrior'.format(col)] = df[col].iloc[0]

    # if no event        
    else:
        account_dict['eventOccurred'] = 0
        account_dict['priorHistory'] = len(df)
        sub_bool = False
        n_bool = False
        for col in event_cols:
            account_dict['{}Event'.format(col)] = np.nan
        for col in loc_cols:
            account_dict['{}LocPrior'.format(col)] = np.nan
        for col in min_max_cols:
            account_dict['{}MinPrior'.format(col)] = np.nan
            account_dict['{}MaxPrior'.format(col)] = np.nan
        for col in all_vals_cols:
            account_dict['{}ValsPrior'.format(col)] = np.nan
        for col in numeric_cols:
            account_dict['{}SumPrior'.format(col)] = np.nan
            account_dict['{}WeightedPrior'.format(col)] = np.nan

        # random set to nan
        account_dict['randomIndex'] = np.nan
        for col in loc_cols:
            account_dict['{}LocRandom'.format(col)] = np.nan
        for col in min_max_cols:
            account_dict['{}MinRandom'.format(col)] = np.nan
            account_dict['{}MaxRandom'.format(col)] = np.nan
        for col in all_vals_cols:
            account_dict['{}ValsRandom'.format(col)] = np.nan
        for col in numeric_cols:
            account_dict['{}SumRandom'.format(col)] = np.nan
            account_dict['{}WeightedRandom'.format(col)] = np.nan

    return account_dict


# In[27]:


log = {}
log['securitization'] = term
master_list = []
broke = 0
status = 'good'
s1 = time.time()
ids_count = 0
for ids in id_lists:
    
    # get update
    ids_count = ids_count + len(ids)
    percent = ids_count / len(all_ids)
    print(ids_count, percent)
    
    # create sub
    sub_df = data[data[id_col].isin(ids)]
    sub_df['indexTransaction'] = sub_df.index
    sub_df = sub_df.reset_index(drop = True)
    splits = list(sub_df.groupby(id_col)) 
    l = [splits[n_][1] for n_ in list(range(len(splits)))]
    a = np.array(l)

    # get results
    s2 = time.time()
    try:
        results = [convert_static(d) for d in a]
        for r in results:
            master_list.append(r)
    except:
        status = 'bad'
        broke = broke + 1
        if broke > 0:
            sys.exit('Too many errors...')
    
    e1 = time.time()
    e2 = time.time()
    log['{}'.format(ids_count)] = (e2 - s2)
    
    print(e2 - s2)
    print(e1 - s1)
    print('-----------------------------')
    


# In[28]:


log['total_time'] = e1 - s1
log['run_status'] = status


# In[29]:


len(all_ids) == len(master_list)


# In[30]:


master = pd.DataFrame(master_list)


# In[31]:


master['accountStatusEvent'].value_counts(dropna = False)


# ### Adding final fields

# In[32]:


master['securitization'] = term


# In[33]:


def get_target(row):
    
    """
    Set target var
    """
    
    init = str(row['accountStatusEvent'])
    remaining = row['remainingTermToMaturityNumberMinPrior']
    #remaining = row['remainingTermToMaturityNumberLocCurrent']
    #remaining = row['remainingTermToMaturityNumberLocPrior']
    
    if init == 'Charged-off':
        res = 'Charged-off'
        return res
    elif init == 'Prepaid or Matured' and remaining > 1:
        res = 'Prepaid'
        return res
    elif init == 'Prepaid or Matured' and remaining < 2:
        res = 'Closed'
        return res
    elif init == 'nan':
        res = 'Active or other'
        return res
    else:
        res = 'Active or other'
        return res
    


# In[34]:


master['target'] = master.apply(get_target, axis = 1)


# In[35]:


master['target'].value_counts(dropna = False)


# In[36]:


master.shape


# In[37]:


#sub = master[['id', 'records', 'reportingPeriodBeginningDateRLocCurrent']]


# ### Export

# In[38]:


e_folder = 'data/static/'
e_file = '{} static.csv'.format(term)
e_path = e_folder + e_file
master.to_csv(e_path, index = False)


# In[39]:


# export log
j_folder = 'data/static/log/'
j_file = '{} log.json'.format(term)
j_path = j_folder + j_file
with open(j_path, 'w') as outfile:  
    json.dump(log, outfile, indent = 4, separators = (',', ': '), sort_keys = False)


# In[40]:


print('continue...')


# In[ ]:





# ### End
