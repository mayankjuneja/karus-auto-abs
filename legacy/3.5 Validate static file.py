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
    


# In[6]:


s_folder = 'data/static/'
s_file = '{} static.csv'.format(term)
s_path = s_folder + s_file
s_path


# In[7]:


static = pd.read_csv(s_path)


# ### Formatting

# In[8]:


def convert_id(row, column):
    
    """
    Convert ids
    """
    
    init = str(row[column])
    
    cleaned = init.replace('=', '').replace('"', '').strip()
    
    return cleaned


# In[9]:


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
        


# In[10]:


data['ID'] = data.parallel_apply(convert_id, args = ('assetNumber', ), axis = 1)


# In[11]:


# date cols
date_cols = ['reportingPeriodBeginningDate', 'reportingPeriodEndingDate', 'originationDate', 'loanMaturityDate', 'originalFirstPaymentDate', 'interestPaidThroughDate', 'zeroBalanceEffectiveDate', 'mostRecentServicingTransferReceivedDate', 'DemandResolutionDate'] 


# In[12]:


for col in date_cols:
    print(col)
    data['{}R'.format(col)] = data.parallel_apply(reorder_date, args = (col, ), axis = 1)
    


# In[13]:


replacer_cols = ['originalLoanAmount', 'originalLoanTerm', 'originalInterestRatePercentage', 'gracePeriodNumber', 'obligorCreditScore', 'paymentToIncomePercentage', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'reportingPeriodInterestRatePercentage', 'nextInterestRatePercentage', 'servicingFeePercentage', 'servicingFlatFeeAmount', 'otherServicerFeeRetainedByServicer', 'otherAssessedUncollectedServicerFeeAmount', 'scheduledInterestAmount', 'scheduledPrincipalAmount', 'otherPrincipalAdjustmentAmount', 'reportingPeriodActualEndBalanceAmount', 'reportingPeriodScheduledPaymentAmount', 'totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount', 'servicerAdvancedAmount', 'currentDelinquencyStatus', 'repurchaseAmount', 'chargedoffPrincipalAmount', 'recoveredAmount', 'repossessedProceedsAmount']    


# In[14]:


data[replacer_cols] = data[replacer_cols].replace('-', np.nan)


# In[15]:


# clean cols
clean_cols = ['currentDelinquencyStatus']
for col in clean_cols:
    data[col] = data[col].str.strip()
    data[col] = data[col].astype(float)


# In[16]:


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


# In[17]:


for col in m_cols:
    print(col)
    new_col = col + 'M'
    data[new_col] = data.parallel_apply(replace_val, args = (col, ), axis = 1)
    


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


# In[22]:


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


# In[23]:


static['target'] = static.apply(get_target, axis = 1)


# ### Applitcaion

# In[48]:


list(data['ID'].unique())


# In[49]:


loc_cols = list(set(['originatorName', 'primaryLoanServicerName', 'originationDateR', 'originalLoanAmount', 'originalLoanTerm', 'loanMaturityDateR', 'originalInterestRatePercentage', 'interestCalculationTypeCodeM', 'originalInterestRateTypeCodeM', 'originalInterestOnlyTermNumber', 'originalFirstPaymentDateR', 'underwritingIndicator', 'paymentTypeCodeM', 'vehicleManufacturerName', 'vehicleModelName', 'vehicleNewUsedCodeM', 'vehicleModelYear', 'vehicleTypeCodeM', 'vehicleValueAmount', 'vehicleValueSourceCodeM', 'obligorCreditScoreType', 'obligorIncomeVerificationLevelCodeM', 'obligorEmploymentVerificationCodeM', 'coObligorIndicator', 'paymentToIncomePercentage', 'obligorGeographicLocation', 'assetAddedIndicator', 'reportingPeriodModificationIndicator', 'servicingAdvanceMethodCodeM', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'reportingPeriodInterestRatePercentage', 'nextInterestRatePercentage', 'scheduledInterestAmount', 'otherPrincipalAdjustmentAmount', 'reportingPeriodActualEndBalanceAmount', 'reportingPeriodScheduledPaymentAmount', 'assetSubjectDemandIndicator', 'zeroBalanceEffectiveDateR']))         
min_max_cols = list(set(['reportingPeriodBeginningDateR', 'reportingPeriodEndingDateR', 'remainingTermToMaturityNumber', 'obligorCreditScore', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue']))
max_cols = list(set(['gracePeriodNumber', 'interestPaidThroughDateR', 'mostRecentServicingTransferReceivedDateR', 'chargedoffPrincipalAmount', 'recoveredAmount', 'paymentExtendedNumber', 'repossessedProceedsAmount', 'currentDelinquencyStatus']))
all_vals = list(set(['subventedM', 'assetSubjectDemandIndicator', 'assetSubjectDemandStatusCodeM', 'repurchaserName', 'repurchaseReplacementReasonCodeM', 'modificationTypeCodeM', 'repossessedIndicator', 'zeroBalanceCodeM', 'accountStatus']))
sum_cols = list(set(['servicingFeePercentage', 'servicingFlatFeeAmount', 'otherServicerFeeRetainedByServicer', 'otherAssessedUncollectedServicerFeeAmount', 'totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount', 'servicerAdvancedAmount', 'currentDelinquencyStatus', 'repurchaseAmount', 'chargedoffPrincipalAmount', 'recoveredAmount', 'paymentExtendedNumber', 'repossessedProceedsAmount', 'repossessedProceedsAmount', 'gracePeriodNumber']))
event_cols = list(set(['accountStatus', 'zeroBalanceCodeM', 'chargedoffPrincipalAmount', 'reportingPeriodBeginningDateR', 'reportingPeriodEndingDateR']))


# In[50]:


id_col = 'ID'
_id = '0001694010 - 000264'
status_col = 'accountStatus'
values = ['Charged-off', 'Prepaid or Matured', 'Repurchased or Replaced']


# In[51]:


df = data[data[id_col] == _id].reset_index(drop = True)
df = df.sort_values('reportingPeriodBeginningDateR', ascending = False)

# dict
account_dict = {}
account_dict['id'] = _id
account_dict['records'] = len(df)

# current status of loan
for col in loc_cols:
    account_dict['{}LocCurrent'.format(col)] = df[col].iloc[0]
for col in min_max_cols:
    _min = df[col].min()
    _max = df[col].max()
    account_dict['{}MinCurrent'.format(col)] = _min
    account_dict['{}MaxCurrent'.format(col)] = _max
for col in max_cols:
    account_dict['{}MaxCurrent'.format(col)] = df[col].max()
for col in all_vals:
    vals = list(df[col].unique())
    use_vals = ' | '.join(str(val) for val in vals)
    account_dict['{}ValsCurrent'.format(col)] = use_vals
for col in sum_cols:
    account_dict['{}SumCurrent'.format(col)] = df[col].sum()

# event information
init_vals = list(df[status_col].unique())
inter = list(set(values).intersection(init_vals))
if len(inter) > 0:
    account_dict['eventOccurred'] = 1
    n = df[status_col].where(df[status_col].isin(values)).last_valid_index()
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
            _min = sub[col].min()
            _max = sub[col].max()
            account_dict['{}MinPrior'.format(col)] = _min
            account_dict['{}MaxPrior'.format(col)] = _max
        for col in max_cols:
            account_dict['{}MaxPrior'.format(col)] = sub[col].max()
        for col in all_vals:
            vals = list(sub[col].unique())
            use_vals = ' | '.join(str(val) for val in vals)
            account_dict['{}ValsPrior'.format(col)] = use_vals
        for col in sum_cols:
            account_dict['{}SumPrior'.format(col)] = sub[col].sum()
            
        # random
        len_sub = len(sub)
        s = random.randint(0, len_sub)
        r_sub = sub[s:len_sub].reset_index(drop = True)
        account_dict['randomIndex'] = s
        for col in loc_cols:
            account_dict['{}LocRandom'.format(col)] = r_sub[col].iloc[0]
        for col in min_max_cols:
            _min = r_sub[col].min()
            _max = r_sub[col].max()
            account_dict['{}MinRandon'.format(col)] = _min
            account_dict['{}MaxRandom'.format(col)] = _max
        for col in max_cols:
            account_dict['{}MaxRandom'.format(col)] = r_sub[col].max()
        for col in all_vals:
            vals = list(r_sub[col].unique())
            use_vals = ' | '.join(str(val) for val in vals)
            account_dict['{}ValsRandom'.format(col)] = use_vals
        for col in sum_cols:
            account_dict['{}SumRandom'.format(col)] = r_sub[col].sum()
            
    # if event is first row of sub       
    else:
        for col in loc_cols:
            account_dict['{}LocPrior'.format(col)] = df[col].iloc[0]
        for col in min_max_cols:
            _min = df[col].min()
            _max = df[col].max()
            account_dict['{}MinPrior'.format(col)] = _min
            account_dict['{}MaxPrior'.format(col)] = _max
        for col in max_cols:
            account_dict['{}MaxPrior'.format(col)] = df[col].max()
        for col in all_vals:
            vals = list(df[col].unique())
            use_vals = ' | '.join(str(val) for val in vals)
            account_dict['{}ValsPrior'.format(col)] = use_vals
        for col in sum_cols:
            account_dict['{}SumPrior'.format(col)] = df[col].sum()

# if no event        
else:
    account_dict['eventOccured'] = 0
    account_dict['priorHistory'] = len(df)
    sub_bool = False
    n_bool = False
    for col in event_cols:
        account_dict['{}Event'.format(col)] = np.nan
    for col in loc_cols:
        account_dict['{}LocPrior'.format(col)] = df[col].iloc[0]
    for col in min_max_cols:
        _min = df[col].min()
        _max = df[col].max()
        account_dict['{}MinPrior'.format(col)] = _min
        account_dict['{}MaxPrior'.format(col)] = _max
    for col in max_cols:
        account_dict['{}MaxPrior'.format(col)] = df[col].max()
    for col in all_vals:
        vals = list(df[col].unique())
        use_vals = ' | '.join(str(val) for val in vals)
        account_dict['{}ValsPrior'.format(col)] = use_vals
    for col in sum_cols:
        account_dict['{}SumPrior'.format(col)] = df[col].sum()
    
    # random set to nan
    account_dict['randomIndex'] = np.nan
    for col in loc_cols:
        account_dict['{}LocRandom'.format(col)] = np.nan
    for col in min_max_cols:
        account_dict['{}MinRandon'.format(col)] = np.nan
        account_dict['{}MaxRandom'.format(col)] = np.nan
    for col in max_cols:
        account_dict['{}MaxRandom'.format(col)] = np.nan
    for col in all_vals:
        account_dict['{}ValsRandom'.format(col)] = np.nan
    for col in sum_cols:
        account_dict['{}SumRandom'.format(col)] = np.nan
    


# In[52]:


account_dict


# In[53]:


len(account_dict)


# In[54]:


res_dict = {}


# In[55]:


for col in loc_cols:
    
    cor = col + 'LocCurrent'
    val = account_dict[cor]
    comp = df[col].iloc[0]
    
    if str(val) == str(comp):
        res = 'Match'
    else:
        res = 'No match'
    
    if res == 'No match':
        print('NO MATCH!!!!!!!')
    
    res_dict[cor] = {'res': res, 'val': val}


# In[56]:


for col in loc_cols:
    
    if sub_bool == True:
        use_df = sub
    elif sub_bool == False:
        use_df = df
    
    cor = col + 'LocPrior'
    val = account_dict[cor]
    comp = use_df[col].iloc[0]
    
    if str(val) == str(comp):
        res = 'Match'
    else:
        res = 'No match'
        
    if res == 'No match':
        print('NO MATCH!!!!!!!')
    
    res_dict[cor] = {'res': res, 'val': val}


# In[57]:


for col in event_cols:
    
    cor = col + 'Event'
    val = account_dict[cor]
    
    if n_bool == True:
        comp = single[col].iloc[0]
    else:
        comp = str(np.nan)
    
    if str(val) == str(comp):
        res = 'Match'
    else:
        res = 'No match'
    
    if res == 'No match':
        print('NO MATCH!!!!!!!')
    
    res_dict[cor] = {'res': res, 'val': val}
    


# In[58]:


for col in loc_cols:
    
    if sub_bool == True:
        use_df = r_sub
    elif sub_bool == False:
        use_df = df
    
    cor = col + 'LocRandom'
    val = account_dict[cor]
    comp = use_df[col].iloc[0]
    
    if str(val) == str(comp):
        res = 'Match'
    else:
        res = 'No match'
        
    if res == 'No match':
        print('NO MATCH!!!!!!!')
    
    res_dict[cor] = {'res': res, 'val': val}


# In[59]:


res_dict


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




