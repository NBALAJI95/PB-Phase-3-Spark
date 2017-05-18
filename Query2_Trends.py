
# coding: utf-8

# In[1]:

dfc = sqlContext.read.load('file:////home/cloudera/Downloads/Q2_INPUT.csv', 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')


# In[13]:

dfc.printSchema()


# In[39]:

dfc.registerAsTable('train_table')
df1 = sqlContext.sql('SELECT HashTag, (MAX(Timestamp) - MIN(Timestamp))/60 AS Life_Time FROM train_table GROUP BY HashTag')
df1.registerAsTable('train_table7')
df1 = sqlContext.sql('SELECT * FROM train_table7 WHERE Life_Time>0')


# In[37]:

df1.count()


# In[16]:

dfc.registerAsTable('train_table')
df2 = sqlContext.sql('SELECT HashTag, COUNT(*) AS Occurance FROM train_table GROUP BY HashTag ORDER BY Occurance DESC')


# In[45]:

df2.registerAsTable('train_table12')
df1.registerAsTable('train_table123')
df3 = sqlContext.sql('SELECT train_table12.HashTag, Life_Time, Occurance, Life_Time / Occurance AS RATE_OF_OCCURANCE FROM train_table12 JOIN train_table123 ON train_table12.HashTag = train_table123.HashTag WHERE Life_Time > 600 ORDER BY RATE_OF_OCCURANCE')


# In[47]:

df3.show()

