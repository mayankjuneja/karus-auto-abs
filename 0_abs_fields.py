#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import json


# ### Create fields

# In[2]:


# init
fields_dict = {}
fields_dict['dates'] = list(set(['reportingPeriodBeginningDate', 'reportingPeriodEndingDate', 'originationDate', 'loanMaturityDate', 'originalFirstPaymentDate', 'interestPaidThroughDate', 'zeroBalanceEffectiveDate', 'mostRecentServicingTransferReceivedDate', 'DemandResolutionDate']))
fields_dict['replace_dash'] = list(set(['originalLoanAmount', 'originalLoanTerm', 'originalInterestRatePercentage', 'gracePeriodNumber', 'obligorCreditScore', 'paymentToIncomePercentage', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'reportingPeriodInterestRatePercentage', 'nextInterestRatePercentage', 'servicingFeePercentage', 'servicingFlatFeeAmount', 'otherServicerFeeRetainedByServicer', 'otherAssessedUncollectedServicerFeeAmount', 'scheduledInterestAmount', 'scheduledPrincipalAmount', 'otherPrincipalAdjustmentAmount', 'reportingPeriodActualEndBalanceAmount', 'reportingPeriodScheduledPaymentAmount', 'totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount', 'servicerAdvancedAmount', 'currentDelinquencyStatus', 'repurchaseAmount', 'chargedoffPrincipalAmount', 'recoveredAmount', 'repossessedProceedsAmount']))
fields_dict['clean'] = list(set(['currentDelinquencyStatus']))
fields_dict['map'] = list(set(['interestCalculationTypeCode','originalInterestRateTypeCode','paymentTypeCode','subvented','vehicleNewUsedCode','vehicleTypeCode','vehicleValueSourceCode','obligorIncomeVerificationLevelCode','obligorEmploymentVerificationCode','servicingAdvanceMethodCode','zeroBalanceCode','assetSubjectDemandStatusCode','repurchaseReplacementReasonCode','modificationTypeCode']))
fields_dict['init_id'] = list(set(['assetNumber']))
fields_dict['loc'] = list(set(['dataset_name', 'assetTypeNumber', 'originatorName', 'originalLoanAmount', 'originalLoanTerm', 'originalInterestRatePercentage', 'originalInterestOnlyTermNumber', 'underwritingIndicator', 'gracePeriodNumber', 'vehicleManufacturerName', 'vehicleModelName', 'vehicleModelYear', 'vehicleValueAmount', 'obligorCreditScoreType', 'obligorCreditScore', 'coObligorIndicator', 'paymentToIncomePercentage', 'obligorGeographicLocation', 'assetAddedIndicator', 'remainingTermToMaturityNumber', 'reportingPeriodModificationIndicator', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'reportingPeriodInterestRatePercentage', 'nextInterestRatePercentage', 'servicingFeePercentage', 'servicingFlatFeeAmount', 'otherServicerFeeRetainedByServicer', 'otherAssessedUncollectedServicerFeeAmount', 'scheduledInterestAmount', 'scheduledPrincipalAmount', 'otherPrincipalAdjustmentAmount', 'reportingPeriodActualEndBalanceAmount', 'reportingPeriodScheduledPaymentAmount', 'reportingPeriodScheduledPaymentAmount', 'totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount', 'servicerAdvancedAmount', 'currentDelinquencyStatus', 'primaryLoanServicerName', 'assetSubjectDemandIndicator', 'repurchaseAmount', 'repurchaserName', 'chargedoffPrincipalAmount', 'recoveredAmount', 'paymentExtendedNumber', 'repossessedIndicator', 'repossessedProceedsAmount']))   
fields_dict['numeric'] = list(set(['currentDelinquencyStatus', 'originalLoanTerm','gracePeriodNumber','obligorCreditScore','remainingTermToMaturityNumber','paymentExtendedNumber','originalLoanAmount','originalInterestRatePercentage','vehicleValueAmount','paymentToIncomePercentage','reportingPeriodBeginningLoanBalanceAmount','nextReportingPeriodPaymentAmountDue','reportingPeriodInterestRatePercentage','nextInterestRatePercentage','servicingFeePercentage','servicingFlatFeeAmount','otherAssessedUncollectedServicerFeeAmount','scheduledInterestAmount','scheduledPrincipalAmount','otherPrincipalAdjustmentAmount','reportingPeriodActualEndBalanceAmount','reportingPeriodScheduledPaymentAmount','totalActualAmountPaid','actualInterestCollectedAmount','actualPrincipalCollectedAmount','actualOtherCollectedAmount','servicerAdvancedAmount','repurchaseAmount','chargedoffPrincipalAmount','recoveredAmount','repossessedProceedsAmount']))
fields_dict['all_vals'] = list(set(['indexAccount', 'indexTransaction', 'accountStatus', 'zeroBalanceCodeM'] + fields_dict['map']))


# In[3]:


# xs
xs_loc = list(set(['interestCalculationTypeCode','originalInterestRateTypeCode','paymentTypeCode','subvented','vehicleNewUsedCode','vehicleTypeCode','vehicleValueSourceCode','obligorIncomeVerificationLevelCode','obligorEmploymentVerificationCode','servicingAdvanceMethodCode','assetSubjectDemandStatusCode', 'originatorName', 'underwritingIndicator', 'vehicleManufacturerName', 'vehicleModelName', 'vehicleModelYear', 'coObligorIndicator', 'obligorGeographicLocation', 'currentDelinquencyStatus', 'originalLoanTerm', 'gracePeriodNumber', 'obligorCreditScore', 'remainingTermToMaturityNumber', 'paymentExtendedNumber', 'originalLoanAmount', 'originalInterestRatePercentage', 'vehicleValueAmount', 'paymentToIncomePercentage', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'reportingPeriodInterestRatePercentage', 'nextInterestRatePercentage', 'servicingFeePercentage', 'servicingFlatFeeAmount', 'otherAssessedUncollectedServicerFeeAmount', 'scheduledInterestAmount', 'scheduledPrincipalAmount', 'otherPrincipalAdjustmentAmount', 'reportingPeriodActualEndBalanceAmount', 'reportingPeriodScheduledPaymentAmount', 'reportingPeriodScheduledPaymentAmount', 'totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount', 'servicerAdvancedAmount']))                       
xs_weighted = list(set(['currentDelinquencyStatus', 'gracePeriodNumber', 'remainingTermToMaturityNumber', 'paymentExtendedNumber', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'otherAssessedUncollectedServicerFeeAmount', 'scheduledInterestAmount', 'scheduledPrincipalAmount', 'reportingPeriodActualEndBalanceAmount', 'reportingPeriodScheduledPaymentAmount', 'totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount']))
xs_min_max = list(set(['currentDelinquencyStatus', 'gracePeriodNumber', 'remainingTermToMaturityNumber', 'paymentExtendedNumber', 'reportingPeriodBeginningLoanBalanceAmount', 'nextReportingPeriodPaymentAmountDue', 'scheduledInterestAmount', 'scheduledPrincipalAmount', 'reportingPeriodActualEndBalanceAmount', 'totalActualAmountPaid', 'actualInterestCollectedAmount', 'actualPrincipalCollectedAmount', 'actualOtherCollectedAmount']))
xs_sum = list(set(['currentDelinquencyStatus', 'gracePeriodNumber', 'paymentExtendedNumber', 'totalActualAmountPaid']))


# In[4]:


# combinations
fields_dict['min_max'] = [f + 'R' for f in fields_dict['dates']] + fields_dict['numeric']
fields_dict['all_loc'] = [f + 'R' for f in fields_dict['dates']] + fields_dict['loc'] + [f + 'M' for f in fields_dict['map']]
fields_dict['event'] = ['accountStatus', 'zeroBalanceCodeM', 'chargedoffPrincipalAmount', 'reportingPeriodBeginningDateR', 'reportingPeriodEndingDateR']       


# In[5]:


# for modeling
fields_dict['modeling'] = {'y': 'target',
                           'x_loc':xs_loc,
                           'x_weight':xs_weighted,
                           'x_min_max':xs_min_max,
                           'x_sum':xs_sum}


# ### Export

# In[6]:


e_folder = 'data/json/fields/'
e_file = 'fields.json'
e_path = e_folder + e_file
e_path


# In[7]:


with open(e_path, 'w') as outfile:  
    json.dump(fields_dict, outfile, indent = 4, separators = (',', ': '), sort_keys = False)
    


# In[8]:


print('complete...')


# In[ ]:





# ### End
