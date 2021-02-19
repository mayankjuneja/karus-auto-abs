#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines import CoxPHFitter
import numpy as np


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


term = 'AmeriCredit Automobile Receivables Trust 2017-1 Data Tape'
folder = 'data/static/'
file = '{} static.csv'.format(term)
path = folder + file
path


# In[4]:


df = pd.read_csv(path)


# In[5]:


#df[df['remainingTerm'].isnull()]


# In[6]:


#df[['remainingTerm', 'originalLoanTermLoc', 'remainingTermToMaturityNumberMin', 'accountStatus']]


# ### Processing

# In[7]:


def get_term(row, accountStatus, remainingTerm, originalLoanTermLoc, remainingTermToMaturityNumberMin):
    
    """
    Get final term
    """
    
    act = row[accountStatus]
    rt = row[remainingTerm]
    ot = row[originalLoanTermLoc]
    rtm = row[remainingTermToMaturityNumberMin]
    
    if act in ['Charged-off', 'Prepaid or Matured']:
        if rt >= 0:
            res = ot - rt
        else:
            res = ot - rt
    else:
        res = ot - rtm
    
    return res
    


# In[43]:


df['finalTerm'] = df.apply(get_term, args = ('accountStatus', 'remainingTerm', 'originalLoanTermLoc', 'remainingTermToMaturityNumberMin', ), axis = 1)  
df['finalTerm'].fillna(0, inplace = True)
df['obligorCreditScoreMax'].replace('None', np.nan, inplace = True)

cols = ['finalTerm', 'currentDelinquencyStatusMax', 'obligorCreditScoreMax']
for col in cols:
    df[col].fillna(0, inplace = True)

df['obligorCreditScoreMax'] = df['obligorCreditScoreMax'].astype(float)


# In[45]:


# df[['remainingTerm', 'originalLoanTermLoc', 'remainingTermToMaturityNumberMin', 'accountStatus', 'finalTerm']].head(100)   


# ### Samples

# In[70]:


df['default'] = df['accountStatus'].apply(lambda x: 1 if x == 'Charged-off' else 0)


# In[71]:


durations = df['finalTerm']
event_observed = df['default'] 

km = KaplanMeierFitter() 
km.fit(durations, event_observed, label='Kaplan Meier Estimate')

km.plot()


# In[72]:


kmf = KaplanMeierFitter() 
T = df['finalTerm']
E = df['default']

groups = df['vehicleManufacturerNameLoc']
ix1 = (groups == 'CHEVROLET')
ix2 = (groups == 'DODGE')
ix3 = (groups == 'NISSAN')

kmf.fit(T[ix1], E[ix1], label='CHEVROLET')
ax = kmf.plot()

kmf.fit(T[ix2], E[ix2], label='DODGE')
ax1 = kmf.plot(ax=ax)

kmf.fit(T[ix3], E[ix3], label='NISSAN')
kmf.plot(ax=ax1)   


# In[73]:


kmf1 = KaplanMeierFitter() 

groups = df['vehicleNewUsedCodeMLoc']   
i1 = (groups == 'New') 
i2 = (groups == 'Used')

kmf1.fit(T[i1], E[i1], label='New')
a1 = kmf1.plot()

kmf1.fit(T[i2], E[i2], label='Used')
kmf1.plot(ax=a1)


# ### Build regression

# In[74]:


# model cols
select_cols = ['finalTerm', 'default', 'originalInterestRatePercentageLoc', 'vehicleValueAmountLoc', 'obligorCreditScoreMax', 'paymentToIncomePercentageLoc', 'vehicleTypeCodeMLoc', 'obligorIncomeVerificationLevelCodeMLoc', 'coObligorIndicatorLoc']        


# In[75]:


df_r = df[select_cols]
df_r.head()


# In[78]:


df_dummy = pd.get_dummies(df_r, drop_first=True)
df_dummy.shape


# In[79]:


# proportional hazard model
cph = CoxPHFitter()
cph.fit(df_dummy, 'finalTerm', event_col='default')
cph.print_summary()


# In[80]:


tr_rows = df_dummy.iloc[0:10]


# In[81]:


cph.predict_survival_function(tr_rows).plot()


# In[82]:


preds = cph.predict_survival_function(df_dummy).T


# In[87]:


final = pd.concat([df, preds], axis = 1)


# In[88]:


final


# In[105]:


vals = [.2, .3, .4, .5, .6, .7, .8, .9]


# In[113]:


holder = []
for val in vals:
    sub = final[final[77.0] <= val]
    _sum = sub['default'].sum()
    dr = _sum / len(sub)
    
    _dict = {}
    _dict['level'] = val
    _dict['dr'] = dr
    holder.append(_dict)


# In[114]:


res = pd.DataFrame(holder)


# In[115]:


res


# In[117]:


final[final[70.0] <= .2]


# In[78]:


print('continue...')


# In[ ]:





# In[ ]:





# In[ ]:





# ### End
