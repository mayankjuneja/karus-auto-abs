#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
import json


# ### Value mapping

# In[2]:


values_dict = {}
values_dict['interestCalculationTypeCode'] = {'1':'Simple',
                                              '98': 'Other'}
values_dict['originalInterestRateTypeCode'] = {'1': 'Fixed',
                                               '2': 'Adjustable',
                                               '98': 'Other'}
values_dict['paymentTypeCode'] = {'1':'Bi-Weekly',
                                  '2': 'Monthly',
                                  '3':'Quarterly',
                                  '4': 'Baloon',
                                  '98': 'Other'}
values_dict['subvented'] = {'0': 'No',
                            '1': 'Yes - Rate Subvention',
                            '2': 'Yes - Cash Rebate',
                            '98': 'Yes - Other'}

values_dict['vehicleNewUsedCode'] = {'1': 'New',
                                     '2': 'Used'}

values_dict['vehicleTypeCode'] = {'1': 'Car',
                                  '2': 'Truck',
                                  '3': 'SUV',
                                  '4': 'Motorcycle',
                                  '98': 'Other',
                                  '99': 'Unknown'}

values_dict['vehicleValueSourceCode'] = {'1': 'Invoice Price',
                                         '2': 'MSRP',
                                         '3': 'Kelly Blue Book',
                                         '98': 'Other'}

values_dict['obligorIncomeVerificationLevelCode'] = {'1': 'Not stated, not verified',
                                                     '2': 'Stated, not verified',
                                                     '3': 'Stated, verified but not to level 4 or level 5',
                                                     '4': 'Stated level 4 verified'}

values_dict['obligorEmploymentVerificationCode'] = {'1': 'Not stated, not verified',
                                                    '2': 'Stated, not verified',
                                                    '3': 'Stated, level 3 verified'}

values_dict['servicingAdvanceMethodCode'] = {'1': 'No advancing',
                                             '2': 'Interest only',
                                             '3': 'Principal only',
                                             '4': 'Principal and Interest',
                                             '99': 'Unavailable'}

values_dict['zeroBalanceCode'] = {'1': 'Prepaid or Matured',
                                  '2': 'Third-Party Sale',
                                  '3': 'Repurchased or Replaced',
                                  '4': 'Charged-off',
                                  '5': 'Servicing Transfer',
                                  '99': 'Unavailable'}

values_dict['assetSubjectDemandStatusCode'] = {'0': 'Asset Pending Repurchase or Replacement',
                                               '1': 'Asset Was Repurchased or Replaced',
                                               '2': 'Demand in Dispute',
                                               '3': 'Demand Withdrawn',
                                               '4': 'Demand Rejected',
                                               '98': 'Other'}

values_dict['repurchaseReplacementReasonCode'] = {'1': 'Fraud',
                                                  '2': 'Early Payment Default',
                                                  '3': 'Other Recourse Obligation',
                                                  '4': 'Reps/Warrants Breach',
                                                  '5': 'Servicer Breach',
                                                  '98': 'Other',
                                                  '99': 'Unknown'}

values_dict['modificationTypeCode'] = {'1': 'APR',
                                       '2': 'Principal',
                                       '3': 'Term',
                                       '4': 'Extension',
                                       '98': 'Other'}


# ### Export

# In[3]:


e_folder = 'data/dictionary/mapper/'
e_file = 'mapper.json'
e_path = e_folder + e_file
e_path


# In[4]:


with open(e_path, 'w') as outfile:  
    json.dump(values_dict, outfile, indent = 4, separators = (',', ': '), sort_keys = False)


# In[5]:


print('complete...')


# In[ ]:





# ### End
