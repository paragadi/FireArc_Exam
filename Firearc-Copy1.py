#!/usr/bin/env python
# coding: utf-8

# In[2]:


pip install google-cloud-bigquery


# In[3]:


import requests 
import pandas as pd


# In[56]:


pip install google-auth


# In[102]:


pip install requests sqlalchemy


# In[97]:


export GOOGLE_APPLICATION_CREDENTIALS="C:\Users\tss\nyc-firedep-1e662ae32f85.json"


# In[35]:



# Retrieve the data
import datetime
url = 'https://data.cityofnewyork.us/resource/8m42-w767.json'
response = requests.get(url)
data = response.json()

# Transform the data into a pandas DataFrame fact a
df = pd.DataFrame(data)
df['incident_datetime']=pd.to_datetime(df['incident_datetime']) # select * from mrr where indcident > (select max(indi ) from dwh)
df['insert_date'] = datetime.datetime.now()
display(df)


# Define BigQuery table ID in the format "project_id.dataset.table"
table_id = 'nyc-firedep.Firedep.incident_landing_zone'

# Load the service account credentials from the JSON key file
credentials = service_account.Credentials.from_service_account_file('C:\\Users\\tss\\nyc-firedep-1e662ae32f85.json')

# Create a BigQuery client
client = bigquery.Client(credentials=credentials)

# Load the data into the landing zone table
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

load_job = client.load_table_from_json(data, table_id, job_config=job_config)
load_job.result()  # Wait for the job to complete

print(f'Data loaded successfully into table {table_id}.')





