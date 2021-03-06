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
import os
from project_functions import get_originator

pandarallel.initialize()


# In[2]:


n = 750
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


term = 'Toyota Auto Receivables 2020-D Owner Trust Data Tape'


# In[4]:


originator = get_originator(term)[0]
finder = re.compile(r'\b\d{4}-[A-Za-z\d]+\b')
init_id = re.search(finder, term)
add_id = init_id.group()
print(originator)
print(add_id)


# In[5]:


# load abs
folder = 'data/karus_datasets/{}/{} {}/'.format(originator, originator, add_id)
file = 'transaction_raw.csv'.format(term)
path = folder + file
data = pd.read_csv(path)
init_shape = data.shape[0]
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
west = fields['regions']['west']
south_west = fields['regions']['south_west']
south_east = fields['regions']['south_east']
mid_west = fields['regions']['mid_west']
north_east = fields['regions']['north_east']


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


data = data.sort_values(by=['reportingPeriodBeginningDateR'], ascending=False)
data = data.drop_duplicates(subset = ['id', 'reportingPeriodBeginningDateR'], keep = 'first')
sec_shape = data.shape[0]
data.shape


# In[13]:


if init_shape == sec_shape:
    dup_rows = False
    print('Matching shapes')
else:
    dup_rows = True 
    print('Duplicate rows')


# In[14]:


# s_col = 'loanMaturityDate'
# t_col = '{}R'.format(s_col)
# data[[s_col, t_col]]


# ### Replacing values

# In[15]:


data[replacer_cols] = data[replacer_cols].replace('-', np.nan)


# In[16]:


# clean cols
for col in clean_cols:
    data[col] = data[col].astype(str)
    data[col] = data[col].str.strip()
    data[col] = data[col].astype(float)
    


# ### Replacing values

# In[17]:


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
    


# In[18]:


for col in m_cols:
    print(col)
    values = data[col].values
    ret_vals = [replace_val(v, col) for v in values]
    data['{}M'.format(col)] = ret_vals
    


# In[19]:


# s_col = 'subvented'
# t_col = '{}M'.format(s_col)
# data[[s_col, t_col]]


# ### Account status

# In[20]:


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
    


# In[21]:


b_col = 'reportingPeriodBeginningLoanBalanceAmount'
e_col = 'nextReportingPeriodPaymentAmountDue'
z_col = 'zeroBalanceCodeM'
thresh = 50


# In[22]:


data['accountStatus'] = data.parallel_apply(acct_status, args = (b_col, e_col, z_col, thresh, ), axis = 1)


# In[23]:


data['accountStatus'].value_counts()


# ### Numeric conversion

# In[24]:


# force convert cols to numeric
for col in numeric_cols:
    print(col)
    data[col] = pd.to_numeric(data[col], errors='coerce')
    


# ### Application

# In[25]:


id_col = 'id'
status_col = 'accountStatus'
values = ['Charged-off', 'Prepaid or Matured', 'Repurchased or Replaced']


# In[26]:


all_ids = list(data[id_col].unique())
#all_ids = all_ids[:1000]
print_vals = list(range(0, len(all_ids), 100))
len(all_ids)


# In[27]:


# break ids into list chunks
num = 1000
id_lists = [all_ids[i:i + num] for i in range(0, len(all_ids), num)]  
#id_lists = [id_lists[-1]]


# In[28]:


len(id_lists)


# In[29]:


def convert_static(df, exception_status):
    
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
    account_dict['exceptionStatus'] = exception_status
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


# In[30]:


log = {}
log['securitization'] = term
master_list = []
broke = 0
cautions = 0
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
        exception_status = False
        results = [convert_static(d, exception_status) for d in a]
        for r in results:
            master_list.append(r)
    except:
        try:
            exception_status = True 
            cautions = cautions + 1
            print('HITTING EXCEPTION')
            for l2 in l:
                res = convert_static(l2, exception_status)
                master_list.append(res)
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
    


# In[31]:


log['total_time'] = e1 - s1
log['run_status'] = status
log['cautions'] = cautions
log['duplicate_rows'] = dup_rows


# In[32]:


master = pd.DataFrame(master_list)
log['loans'] = len(master)


# In[33]:


cs = master['obligorCreditScoreLocCurrent'].mean()
if cs <= 620:
    rating = 'sub_prime'
elif cs > 620 and cs <= 720:
    rating = 'near_prime'
elif cs > 720:
    rating = 'prime'
else:
    rating = 'other'
log['average_credit'] = cs
log['rating'] = rating


# In[34]:


len(all_ids) == len(master_list)


# In[35]:


master['accountStatusEvent'].value_counts(dropna = False)


# ### Adding final fields

# In[36]:


master['securitization'] = term


# In[37]:


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
    


# In[38]:


master['target'] = master.apply(get_target, axis = 1)


# In[39]:


master['target'].value_counts(dropna = False)


# In[40]:


def finding_regions(state):
    
    """
    Get region
    """
    
    if state in west:
        return 'West'
    elif state in south_west:
        return 'SouthWest'
    elif state in south_east:
        return 'SouthEast'
    elif state in mid_west:
        return 'MidWest'
    elif state in north_east:
        return 'NorthEast'
    else:
        return 'Unknown'
    


# In[41]:


master['region'] = np.nan
    
master['region'] = master['obligorGeographicLocationLocCurrent'].apply(finding_regions)


# In[42]:


def get_or_year(init):
    
    """
    Get origination year
    """
    
    year = init[0:4]
    
    return year


# In[43]:


year_col = 'originationDateRLocCurrent'

year_vals = master[year_col].values
years = [get_or_year(y) for y in year_vals]
master['originationYear'] = years


# In[44]:


master['originationDate'] = pd.to_datetime(master['originationDateRLocCurrent'])


# In[45]:


master.shape


# In[46]:


#master[master['exceptionStatus'].isin([True])]


# ### Adding outcome for transaction file

# In[47]:


m_col = 'id'
m_type = 'left'
merged = pd.merge(data, master[[m_col, 'target']], on = m_col, how = m_type)
ft_shape = merged.shape[0]


# In[48]:


ft_shape


# In[49]:


final_bool = init_shape == sec_shape == ft_shape
log['lengths_match'] = final_bool


# In[50]:


final_bool


# ### Vals cols

# In[51]:


vals_cols = [col for col in list(master.columns) if 'vals' in col.lower()]


# In[52]:


def fix_vals(row, column):

    """
    Fix column values
    """

    init = str(row[column])

    ret_val = 'str: ' + init

    return ret_val


# In[53]:


for col in vals_cols:
    print(col)
    master[col] = master[col].astype(str)
    master[col] = 'str: ' + master[col]


# ### Export

# In[54]:


export_folder = 'data/karus_datasets/{}/{} {}/'.format(originator, originator, add_id)
export_folder


# In[55]:


path_bool = os.path.isdir(export_folder)
path_bool


# In[56]:


if path_bool == False:
    os.mkir(export_folder)
    print('dir folder')
else:
    print('dir exists')


# In[57]:


#e_folder = 'data/static/'
e_file = 'static.csv'.format(add_id)
e_path = export_folder + e_file
print(e_path)
print(master.shape)
master.to_csv(e_path, index = False)


# In[58]:


#t_folder = 'data/transaction/prepared/'
t_file = 'transaction.csv'.format(add_id)
t_path = export_folder + t_file
print(t_path)
print(merged.shape)
merged.to_csv(t_path)


# In[59]:


# export log
#j_folder = 'data/static/log/'
j_file = '{} log.json'.format(term)
j_path = export_folder + j_file
with open(j_path, 'w') as outfile:  
    json.dump(log, outfile, indent = 4, separators = (',', ': '), sort_keys = False)


# In[60]:


print('continue...')


# In[ ]:





# ### End
