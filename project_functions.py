#!/usr/bin/env python
# coding: utf-8

# ### Project functions

# In[1]:


# get originator
def get_originator(sec):
    
    """
    Get originator
    """
    
    sec = sec.lower()
    if 'carmax' in sec:
        o = 'CarMax'
    elif 'volkswagen' in sec:
        o = 'Volkswagen'
    elif 'ally' in sec:
        o = 'Ally'
    elif 'hyundai' in sec:
        o = 'Hyundai'
    elif 'bmw' in sec:
        o = 'BMW'
    elif 'nissan' in sec:
        o = 'Nissan'
    elif 'americredit' in sec:
        o = 'AmeriCredit'
    elif 'honda' in sec:
        o = 'Honda'
    elif 'gm' in sec:
        o = 'GM'
    elif 'world omni' in sec:
        o = 'World_Omni'
    elif 'drive auto' in sec and 'santander' not in sec:
        o = 'Drive_Auto'
    elif 'usaa' in sec:
        o = 'USAA'
    elif 'toyota' in sec:
        o = 'Toyota'
    elif 'ford' in sec:
        o = 'Ford'
    elif 'harley-davidson' in sec:
        o = 'Harley_Davidson'
    elif 'santander' in sec:
        o = 'Santander'
    elif 'mercedes' in sec or 'mercedez' in sec:
        o = 'Mercedes'
    elif 'capital one' in sec:
        o = 'Capital_One'
    elif 'california' in sec:
        o = 'California_Republic'
    elif 'exeter' in sec:
        o = 'Exeter'
    elif 'fifth third' in sec:
        o = 'Fifth_Third'
    elif 'wells fargo' in sec:
        o = 'Wells_Fargo'
    elif 'carvana' in sec:
        o = 'Carvana'
    else:
        o = sec.split()[0].capitalize()
    
    if 'lease' in sec:
        t = 'lease'
    else:
        t = 'receivables'
    
    return o, t


# In[ ]:





# In[ ]:





# ### End
