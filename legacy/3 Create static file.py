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

pandarallel.initialize()


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


term = 'CarMax Auto Owner Trust 2017-1 Data Tape'
finder = re.compile('\d{4,}\W\d{1,}')
add_id = re.findall(finder, term)[0]
add_id


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
    final = cleaned + '-' + add_id
    
    return final
    


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


loc_cols = list(set(['originatorName', 'primaryLoanServicerName', 'originationDateR', 'originalLoanAmount', 'originalLoanTerm', 'loanMaturityDateR', 'originalInterestRatePercentage', 'interestCalculationTypeCodeM', 'originalInterestRateTypeCodeM', 'originalInterestOnlyTermNumber', 'originalFirstPaymentDateR', 'underwritingIndicator', 'paymentTypeCodeM', 'vehicleManufacturerName', 'vehicleModelName', 'vehicleNewUsedCodeM', 'vehicleModelYear', 'vehicleTypeCodeM', 'vehicleValueAmount', 'vehicleValueSourceCodeM', 'obligorCreditScoreType', 'obligorIncomeVerificationLevelCodeM', 'obligorEmploymentVerificationCodeM', 'coObligorIndicator', 'paymentToIncomePercentage', 'obligorGeographicLocation', 'assetAddedIndicator', 'reportingPeriodModificationIndicator', 'servicingAdvanceMethodCodeM', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'reportingPeriodInterestRatePercentage', 'nextInterestRatePercentage', 'scheduledInterestAmount', 'otherPrincipalAdjustmentAmount', 'reportingPeriodActualEndBalanceAmount', 'reportingPeriodScheduledPaymentAmount', 'assetSubjectDemandIndicator', 'zeroBalanceEffectiveDateR']))         
min_max_cols = list(set(['reportingPeriodBeginningDateR', 'reportingPeriodEndingDateR', 'remainingTermToMaturityNumber', 'obligorCreditScore', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue']))
max_cols = list(set(['gracePeriodNumber', 'interestPaidThroughDateR', 'mostRecentServicingTransferReceivedDateR', 'chargedoffPrincipalAmount', 'recoveredAmount', 'paymentExtendedNumber', 'repossessedProceedsAmount', 'currentDelinquencyStatus']))
all_vals = list(set(['subventedM', 'assetSubjectDemandIndicator', 'assetSubjectDemandStatusCodeM', 'repurchaserName', 'repurchaseReplacementReasonCodeM', 'modificationTypeCodeM', 'repossessedIndicator', 'zeroBalanceCodeM', 'accountStatus']))
sum_cols = list(set(['servicingFeePercentage', 'servicingFlatFeeAmount', 'otherServicerFeeRetainedByServicer', 'otherAssessedUncollectedServicerFeeAmount', 'totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount', 'servicerAdvancedAmount', 'currentDelinquencyStatus', 'repurchaseAmount', 'chargedoffPrincipalAmount', 'recoveredAmount', 'paymentExtendedNumber', 'repossessedProceedsAmount', 'repossessedProceedsAmount', 'gracePeriodNumber']))
event_cols = list(set(['accountStatus', 'zeroBalanceCodeM', 'chargedoffPrincipalAmount', 'reportingPeriodBeginningDateR', 'reportingPeriodEndingDateR']))
all_numeric = list(set(min_max_cols + max_cols + sum_cols))


# In[22]:


# force convert cols to numeric
for col in all_numeric:
    print(col)
    data[col] = pd.to_numeric(data[col],errors='coerce')


# In[23]:


#_id = '0001694010 - 000010'
#all_ids = ['0001694010 - 000010']
id_col = 'ID'
status_col = 'accountStatus'
values = ['Charged-off', 'Prepaid or Matured', 'Repurchased or Replaced']


# In[24]:


holder = []
counter = 0

for _id in all_ids:
    
    try:
        #print(_id)
        counter = counter + 1
        if counter in print_vals:
            print(counter, counter/len(all_ids))
            print('------------------------------')

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
                if s == len_sub:
                    s = s -1
                r_sub = sub[s:len_sub].reset_index(drop = True)
                account_dict['randomIndex'] = s
                for col in loc_cols:
                    account_dict['{}LocRandom'.format(col)] = r_sub[col].iloc[0]
                for col in min_max_cols:
                    _min = r_sub[col].min()
                    _max = r_sub[col].max()
                    account_dict['{}MinRandom'.format(col)] = _min
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
            account_dict['eventOccurred'] = 0
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
                account_dict['{}MinRandom'.format(col)] = np.nan
                account_dict['{}MaxRandom'.format(col)] = np.nan
            for col in max_cols:
                account_dict['{}MaxRandom'.format(col)] = np.nan
            for col in all_vals:
                account_dict['{}ValsRandom'.format(col)] = np.nan
            for col in sum_cols:
                account_dict['{}SumRandom'.format(col)] = np.nan

        holder.append(account_dict)
    except:
        print('ERROR {}'.format(_id))


# In[25]:


master = pd.DataFrame(holder)


# In[26]:


master['accountStatusEvent'].value_counts(dropna = False)


# In[27]:


master.head()


# In[31]:


master['securitization'] = term


# ### Export

# In[32]:


e_folder = 'data/static/'
e_file = '{} static.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[33]:


master.to_csv(e_path, index = False)


# In[34]:


print('continue...')


# In[ ]:





# ### End
