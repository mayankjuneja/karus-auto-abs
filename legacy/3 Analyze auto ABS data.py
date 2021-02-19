#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
import json
import numpy as np
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


# ### Create static file

# In[20]:


iloc_cols = ['originatorName', 'primaryLoanServicerName', 'originationDateR', 'originalLoanAmount', 'originalLoanTerm', 'loanMaturityDateR', 'originalInterestRatePercentage', 'interestCalculationTypeCodeM', 'originalInterestRateTypeCodeM', 'originalInterestOnlyTermNumber', 'originalFirstPaymentDateR', 'underwritingIndicator', 'paymentTypeCodeM', 'vehicleManufacturerName', 'vehicleModelName', 'vehicleNewUsedCodeM', 'vehicleModelYear', 'vehicleTypeCodeM', 'vehicleValueAmount', 'vehicleValueSourceCodeM', 'obligorCreditScoreType', 'obligorIncomeVerificationLevelCodeM', 'obligorEmploymentVerificationCodeM', 'coObligorIndicator', 'paymentToIncomePercentage', 'obligorGeographicLocation', 'assetAddedIndicator', 'reportingPeriodModificationIndicator', 'servicingAdvanceMethodCodeM', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'reportingPeriodInterestRatePercentage', 'nextInterestRatePercentage', 'scheduledInterestAmount', 'otherPrincipalAdjustmentAmount', 'reportingPeriodActualEndBalanceAmount', 'reportingPeriodScheduledPaymentAmount', 'assetSubjectDemandIndicator', 'zeroBalanceEffectiveDateR']         
min_max_cols = ['reportingPeriodBeginningDateR', 'reportingPeriodEndingDateR', 'remainingTermToMaturityNumber', 'currentDelinquencyStatus', 'obligorCreditScore']
max_cols = ['gracePeriodNumber', 'interestPaidThroughDateR', 'mostRecentServicingTransferReceivedDateR', 'chargedoffPrincipalAmount', 'recoveredAmount', 'paymentExtendedNumber', 'repossessedProceedsAmount']
all_vals = ['subventedM', 'assetSubjectDemandIndicator', 'assetSubjectDemandStatusCodeM', 'repurchaserName', 'repurchaseReplacementReasonCodeM', 'modificationTypeCodeM', 'repossessedIndicator', 'zeroBalanceCodeM', 'accountStatus']
mean_cols = ['servicingFeePercentage', 'servicingFlatFeeAmount', 'otherServicerFeeRetainedByServicer', 'otherAssessedUncollectedServicerFeeAmount']
sum_cols = ['totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount', 'servicerAdvancedAmount', 'currentDelinquencyStatus', 'repurchaseAmount', 'chargedoffPrincipalAmount', 'recoveredAmount', 'paymentExtendedNumber', 'repossessedProceedsAmount', 'repossessedProceedsAmount']


# In[21]:


def get_outcome(df):
    
    """
    Find remaining term
    """
    
    df = df.reset_index(drop = True)
    df = df.sort_values('reportingPeriodBeginningDateR', ascending = False)
    
    init_value = df['accountStatus'].iloc[0]
    if init_value in ['Prepaid or Matured', 'Charged-off', 'Repurchased or Replaced']:
        try:
            for idx, row in df.iterrows():
                row_val = row['accountStatus']
                next_val = df['accountStatus'].iloc[idx + 1]
                init_list = ['Unavailable', 'Charged-off', 'Prepaid or Matured']
                init_list.remove(row_val)
                if row_val == init_value and next_val in init_list:
                    return_val = df['remainingTermToMaturityNumber'].iloc[idx + 1]
                    return init_value, return_val
        except:
            return init_value, np.nan
    else:
        return init_value, np.nan
    


# In[22]:


def reorganize(df, id_col, _id):
    
    """
    Static abs files
    """
    
    sub_df = df[df[id_col] == _id].reset_index(drop = True)
    _len = len(sub_df)
    
    for idx, row in sub_df.iterrows():
        val = row['accountStatus']
        if val in ['Charged-off', 'Prepaid or Matured', 'Repurchased or Replaced']:
            use_df = sub_df[idx:len(sub_df)].copy()
            break
    else:
        use_df = sub_df
    
    act_status, timing = get_outcome(use_df)
    
    reorder_dict = {}
    reorder_dict['ID'] = _id
    reorder_dict['records'] = _len
    reorder_dict['accountStatus'] = act_status
    reorder_dict['remainingTerm'] = timing
    
    for col in iloc_cols:
        reorder_dict['{}Loc'.format(col)] = sub_df[col].iloc[0]
    
    for col in min_max_cols:
        _min = sub_df[col].min()
        _max = sub_df[col].max()
        reorder_dict['{}Min'.format(col)] = _min
        reorder_dict['{}Max'.format(col)] = _max
    
    for col in max_cols:
        reorder_dict['{}Max'.format(col)] = sub_df[col].max()
    
    for col in all_vals:
        vals = list(sub_df[col].unique())
        use_vals = ' | '.join(str(val) for val in vals)
        reorder_dict['{}Vals'.format(col)] = use_vals
    
    for col in mean_cols:
        reorder_dict['{}Mean'.format(col)] = sub_df[col].mean()
    
    for col in sum_cols:
        reorder_dict['{}Sum'.format(col)] = sub_df[col].sum()
    
    df = pd.DataFrame(reorder_dict, index = [0])
    
    return df
        


# ### Application

# In[23]:


all_ids = list(data['ID'].unique())
print_vals = list(range(0, len(all_ids), 100))


# In[24]:


broke = []
master = pd.DataFrame()
count = 0
for _id in all_ids:
    #print(_id)
    try:
        df = reorganize(data, 'ID', _id)
    except:
        print("can't parse {}".format(_id))
        broke.append(_id)
    count = count + 1
    master = master.append(df).reset_index(drop = True)
    if count in print_vals:
        print(count)
        print(count / len(all_ids))
        print('---------------------------------------')
        


# ### Validation

# In[29]:


master


# In[30]:


master['accountStatus'].value_counts()


# In[37]:


review_cols = ['ID', 'reportingPeriodBeginningDate', 'reportingPeriodEndingDate', 'originationDate', 'originalLoanTerm', 'originalLoanAmount', 'loanMaturityDate', 'remainingTermToMaturityNumber', 'reportingPeriodBeginningLoanBalanceAmount', 'reportingPeriodBeginningLoanBalanceAmount', 'totalActualAmountPaid', 'zeroBalanceEffectiveDate', 'chargedoffPrincipalAmount', 'repossessedIndicator', 'recoveredAmount', 'zeroBalanceCodeM', 'currentDelinquencyStatus', 'accountStatus', 'reportingPeriodInterestRatePercentage']             
sub_df = data[data['zeroBalanceCodeM'] == 'Prepaid or Matured']
ids = list(sub_df['ID'].unique())
_id = master['ID'].iloc[118]
sub = data[data['ID'] == _id].reset_index(drop = True)
sub[review_cols]


# ### Export

# In[33]:


e_folder = 'data/static/'
e_file = '{} static.csv'.format(term)
e_path = e_folder + e_file
e_path


# In[34]:


master.to_csv(e_path, index = False)


# In[35]:


print('continue...')


# In[ ]:





# In[ ]:





# ### End
