#!/usr/bin/env python
# coding: utf-8

# # install the library

# In[1]:


get_ipython().system('pip install psycopg2')


# # import psycopg2

# In[2]:


import psycopg2


# # create a connection to database
# 

# In[3]:


try:
    connection= psycopg2.connect("host=localhost dbname=postgres user=postgres password=Siva9908$")
except:
    print("could not make a connection");


# # create a cursor to execute the quieries

# In[4]:


cur=connection.cursor();


# # set autocommit to true

# In[5]:


connection.set_session(autocommit=True);


# # create a DB

# In[6]:


cur.execute("create database first");


# # now close the connection to postgres db and get the connection to the database you created and create a new cursor

# In[7]:


cur.close()


# In[8]:


conn=psycopg2.connect("host=localhost dbname=first user=postgres password=Siva9908$")


# In[9]:


kar=conn.cursor();


# In[10]:


conn.set_session(autocommit=True);


# # now create a table for students

# In[11]:


kar.execute("create table IF NOT EXISTS students (student_id int,name varchar,age int,gender varchar,subject varchar,marks int);");


# # insert data in the table

# In[12]:


kar.execute("INSERT INTO students(student_id,name,age,gender,subject,marks) VALUES (1,'raj',23,'male','python',85)")


# In[13]:


kar.execute("INSERT INTO students(student_id,name,age,gender,subject,marks) VALUES (2,'priya',22,'female','python',86)")


# # validate the data

# In[14]:


kar.execute("select * from students")

row=kar.fetchone()
while row:
    print(row)
    row=kar.fetchone()


# # close the connection

# In[15]:


kar.close();
conn.close();


# In[ ]:




