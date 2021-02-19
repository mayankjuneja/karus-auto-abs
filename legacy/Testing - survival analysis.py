#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[1]:


import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines import CoxPHFitter


# In[2]:


n = 500
pd.set_option('display.max_columns', n)
pd.set_option('display.max_rows', n)
pd.set_option('display.max_colwidth', -1)


# ### Load data

# In[3]:


folder = 'data/test survival/'
file = 'WA_Fn-UseC_-Telco-Customer-Churn.csv'
path = folder + file
path


# In[4]:


df = pd.read_csv(path)


# In[5]:


df.head(2)


# ### Processing

# In[6]:


# convert
df['TotalCharges']=pd.to_numeric(df['TotalCharges'],errors='coerce')
df['Churn']=df['Churn'].apply(lambda x: 1 if x == 'Yes' else 0 )
df.TotalCharges.fillna(value=df['TotalCharges'].median(),inplace=True)


# In[7]:


cat_cols = [i for i in df.columns if df[i].dtype==object]
cat_cols.remove('customerID')


# In[8]:


durations = df['tenure']
event_observed = df['Churn'] 

km = KaplanMeierFitter() 
km.fit(durations, event_observed,label='Kaplan Meier Estimate')

km.plot()


# In[9]:


kmf = KaplanMeierFitter() 
T = df['tenure']
E = df['Churn']

groups = df['Contract']
ix1 = (groups == 'Month-to-month')
ix2 = (groups == 'Two year')
ix3 = (groups == 'One year')

kmf.fit(T[ix1], E[ix1], label='Month-to-month')
ax = kmf.plot()

kmf.fit(T[ix2], E[ix2], label='Two year')
ax1 = kmf.plot(ax=ax)

kmf.fit(T[ix3], E[ix3], label='One year')
kmf.plot(ax=ax1)   


# In[10]:


kmf1 = KaplanMeierFitter() 

groups = df['StreamingTV']   
i1 = (groups == 'No') 
i2 = (groups == 'Yes')

kmf1.fit(T[i1], E[i1], label='Not Subscribed StreamingTV')
a1 = kmf1.plot()

kmf1.fit(T[i2], E[i2], label='Subscribed StreamingTV')
kmf1.plot(ax=a1)


# In[11]:


df_r= df.loc[:,['tenure','Churn','gender','Partner','Dependents','PhoneService','MonthlyCharges','SeniorCitizen','StreamingTV']]
df_r.head() ## have a look at the data


# In[12]:


df_dummy = pd.get_dummies(df_r, drop_first=True)
df_dummy.head()


# In[14]:


# Using Cox Proportional Hazards model
cph = CoxPHFitter()   ## Instantiate the class to create a cph object
cph.fit(df_dummy, 'tenure', event_col='Churn')   ## Fit the data to train the model
cph.print_summary()    ## HAve a look at the significance of the features


# In[15]:


tr_rows = df_dummy.iloc[5:10, 2:]
tr_rows


# In[16]:


cph.predict_survival_function(tr_rows).plot()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




