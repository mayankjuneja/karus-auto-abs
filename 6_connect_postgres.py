#!/usr/bin/env python
# coding: utf-8

# ### Load libs

# In[23]:


import sqlalchemy
import time
import pandas as pd


# ### Create connection

# In[13]:


conn = sqlalchemy.create_engine(sqlalchemy.engine.url.URL(
drivername="postgresql",
    username="postgres",
    password="0KGy0zzND6N8avLl",
    host="localhost",
    port="5432",
    database="postgres"
))


# ### Query

# In[26]:


s = time.time()
df = pd.read_sql(sql="select * from americredit", con=conn)
e = time.time()
pt = e - s
pt


# In[27]:


print('continue...')


# In[ ]:





# ### End
