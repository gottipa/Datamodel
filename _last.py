#!/usr/bin/env python
# coding: utf-8

# In[18]:


import pandas as pd
import psycopg2


# # define a function to create a database and cursor and auto commit

# In[22]:


def createdatabase():
    conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=Siva9908$");
    cur=conn.cursor();
    conn.set_session(autocommit=True)
    
    #create a database named accounts
    cur.execute("CREATE DATABASE accounts");
    
    #now close the connection
    conn.close();
    
    connection=psycopg2.connect("host=localhost dbname=accounts user=postgres password=Siva9908$");
    kar=connection.cursor();
    conn.set_session(autocommit=True);
    
    return kar,connection
    


# # read the data from csv file

# In[ ]:


acountry = pd.read_csv("data/wealth-Accountscountry.csv")


# # to get the top 5 rows

# In[ ]:


acountry.head();


# # to get particular columns in the csv file

# In[ ]:


acountry_clean=acountry[['country code','short name','table name','long name','currency unit']];


# # read the 2nd csv file as well

# In[ ]:


adata=pd.read_csv("data/wealth-Accountsdata.csv")


# # to remove unwanted colums

# In[ ]:


adata.columns


# # the above code gets the all the colums

# In[ ]:


adata_clean=adata.drop(['unmaned:9'],axis=1)
#axis here represents the columns if we want rows put axis=0


# In[ ]:


adata_clean.head();


# # read the 3rd csv file

# In[ ]:


aseries=pd.read_csv("data/wealth-Accountsseries.csv")


# In[ ]:


aseries.columns()


# # call the create database function

# In[ ]:


cur,conn=createdatabase();


# # create the tables

# In[ ]:


s=("""CREATE TABLES IF NOT EXISTS accountscountry(
country_code VARCHAR PRIMARY KEY,
short_name VARCHAR,
table_name VARCHAR,
long_name VARCHAR,
currency_unit VARCHAR
)""")


# In[ ]:


a=("""CREATE TABLES IF NOT EXISTS accountsdata(
country_name VARCHAR,
country_code VARCHAR,
indicator_name VARCHAR,
indicator_code VARCHAR,
year_1995 numeric,
year_2000 numeric,
year_2005 numeric,
year_2010 numeric,
year_2014 numeric)""")


# In[ ]:


b=("""CREATE TABLES IF NOT EXISTS accountseries(
series_code VARCHAR,
topic VARCHAR,
indicator_name VARCHAR,
short_definition VARCHAR)""")


# In[ ]:


cur.execute(s);
cur.execute(a);
cur.execute(b)
conn.commit()


# # inset data into the tables

# In[ ]:


s_insert=("""INSERT INTO s(country_code,
short_name,
table_name,
long_name,
currency_unit)
VALUES (%s,%s,%s,%s,%s)""")


# In[ ]:


for i,row in acountry_clean.itterows():
    cur.execute(s,list(row))


# In[ ]:


conn.commit();


# In[ ]:


a_insert=("""INSERT INTO a(country_name,
country_code,
indicator_name,
indicator_code,
year_1995,
year_2000,
year_2005,
year_2010,
year_2014)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""")


# In[ ]:


for i,row in adata_clean.itterows():
    cur.execute(a,list(row));


# In[ ]:


b_insert=("""INSERT INTO b(series_code,
topic,
indicator_name,
short_definition)
VALUES(%s,%s,%s,%s)""")


# In[ ]:


for i,row in aseries.itterows():
    cur.execute(b,list(row));


# In[ ]:


conn.commit();

