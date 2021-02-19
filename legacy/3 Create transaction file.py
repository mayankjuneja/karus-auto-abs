#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
import json
import numpy as np
import random
from pandarallel import pandarallel

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
folder = 'data/combined/'
file = '{}.csv'.format(term)
path = folder + file
data = pd.read_csv(path)
data.shape


# In[5]:


# load mapper
m_folder = 'data/dictionary/mapper/'
m_file = 'mapper.json'
m_path = m_folder + m_file
with open(m_path) as f:
    mapper = json.load(f)
m_cols = list(mapper.keys())
    


# ### Formatting

# In[6]:


def convert_id(row, column):
    
    """
    Convert ids
    """
    
    init = str(row[column])
    
    cleaned = init.replace('=', '').replace('"', '').strip()
    
    return cleaned
    


# In[7]:


def reorder_date(row, column):
    
    """
    Reorder date
    """
    
    init = str(row[column])
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
        


# In[8]:


data['ID'] = data.parallel_apply(convert_id, args = ('assetNumber', ), axis = 1)


# In[9]:


# date cols
date_cols = ['reportingPeriodBeginningDate', 'reportingPeriodEndingDate', 'originationDate', 'loanMaturityDate', 'originalFirstPaymentDate', 'interestPaidThroughDate', 'zeroBalanceEffectiveDate', 'mostRecentServicingTransferReceivedDate', 'DemandResolutionDate'] 


# In[10]:


for col in date_cols:
    print(col)
    data['{}R'.format(col)] = data.parallel_apply(reorder_date, args = (col, ), axis = 1)
    


# In[11]:


replacer_cols = ['originalLoanAmount', 'originalLoanTerm', 'originalInterestRatePercentage', 'gracePeriodNumber', 'obligorCreditScore', 'paymentToIncomePercentage', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'reportingPeriodInterestRatePercentage', 'nextInterestRatePercentage', 'servicingFeePercentage', 'servicingFlatFeeAmount', 'otherServicerFeeRetainedByServicer', 'otherAssessedUncollectedServicerFeeAmount', 'scheduledInterestAmount', 'scheduledPrincipalAmount', 'otherPrincipalAdjustmentAmount', 'reportingPeriodActualEndBalanceAmount', 'reportingPeriodScheduledPaymentAmount', 'totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount', 'servicerAdvancedAmount', 'currentDelinquencyStatus', 'repurchaseAmount', 'chargedoffPrincipalAmount', 'recoveredAmount', 'repossessedProceedsAmount']    


# In[12]:


data[replacer_cols] = data[replacer_cols].replace('-', np.nan)


# In[13]:


# clean cols
clean_cols = ['currentDelinquencyStatus']
for col in clean_cols:
    data[col] = data[col].str.strip()
    data[col] = data[col].astype(float)
    


# In[14]:


def replace_val(row, column):
    
    """
    Replace numeric values
    """
    
    init = str(row[column]).strip().replace(';', '')
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
    


# In[15]:


for col in m_cols:
    print(col)
    new_col = col + 'M'
    data[new_col] = data.parallel_apply(replace_val, args = (col, ), axis = 1)
    


# In[16]:


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
    


# In[17]:


b_col = 'reportingPeriodBeginningLoanBalanceAmount'
e_col = 'nextReportingPeriodPaymentAmountDue'
z_col = 'zeroBalanceCodeM'
thresh = 50


# In[18]:


data['accountStatus'] = data.parallel_apply(acct_status, args = (b_col, e_col, z_col, thresh, ), axis = 1)


# In[19]:


data['accountStatus'].value_counts()


# ### Application

# In[20]:


all_ids = list(data['ID'].unique())
print_vals = list(range(0, len(all_ids), 100))


# In[21]:


#_id = '0001694010 - 000010'
#all_ids = ['0001694010 - 000010', '0001694010 - 000088', '0001694010 - 009321']
sum_cols = ['chargedoffPrincipalAmount', 'recoveredAmount', 'repossessedProceedsAmount']
id_col = 'ID'
status_col = 'accountStatus'
values = ['Charged-off', 'Prepaid or Matured', 'Repurchased or Replaced']


# In[22]:


holder = []
counter = 0

for _id in all_ids:
    #print(_id)
    counter = counter + 1
    if counter in print_vals:
        print(counter, counter/len(data))
        print('------------------------------')
    
    df = data[data[id_col] == _id].reset_index(drop = True)
    df = df.sort_values('reportingPeriodBeginningDateR', ascending = False)
    for col in sum_cols:
        df['{}Sum'.format(col)] = df[col].sum()
    
    init_vals = list(df[status_col].unique())
    inter = list(set(values).intersection(init_vals))
    
    if len(inter) > 0:
        n = df[status_col].where(df[status_col].isin(values)).last_valid_index()
        sub = df[n:len(df)]
        sub.reset_index(drop = True, inplace = True)
        holder.append(sub)
    else:
        df.reset_index(drop = True, inplace = True)
        holder.append(df)


# In[23]:


master = pd.concat(holder)


# In[24]:


master = master.reset_index(drop = True)


# ### Add target

# In[25]:


master['previousRemainingTerm'] = master['remainingTermToMaturityNumber'].shift(-1)


# In[26]:


def get_target(row):
    
    """
    Set target var
    """
    
    init = str(row['accountStatus'])
    remaining = row['previousRemainingTerm']
    
    if init == 'Charged-off':
        res = 'Charged-off'
        return res

    if init == 'Prepaid or Matured' and remaining > 0:
        res = 'Prepaid'
        return res
    
    if init == 'Prepaid or Matured' and remaining < 1:
        res = 'Closed'
        return res
    
    if init == 'Unavailable':
        res = 'Active or other'
        return res
    


# In[27]:


master['target'] = master.apply(get_target, axis = 1)


# In[28]:


master['target'].value_counts()


# ### Export

# In[29]:


e_folder = 'data/transaction/'
e_file = '{} transaction prepared.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[30]:


master.to_csv(e_path, index = False)


# In[31]:


print('continue...')


# In[ ]:





# ### End
