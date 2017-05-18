
# coding: utf-8

# In[2]:

dfc = sqlContext.read.load('file:////home/cloudera/Downloads/INPUT(2).csv', 
                          format='com.databricks.spark.csv', 
                          header='true', 
                          inferSchema='true')


# In[7]:

dfc.show()


# In[24]:

dfc.registerAsTable('train_table')
df12 = sqlContext.sql('SELECT * from train_table WHERE Follower_Count > 4000 AND Verified = "True" ORDER BY Follower_Count desc')
df12.show(10)


# In[16]:

df12.registerAsTable('train_table1')
df1 = sqlContext.sql('SELECT User_Screen_Name, MAX(Follower_Count) - MIN(Follower_Count) AS FOLLOWER_COUNT_INCREASE from train_table1 GROUP BY User_Screen_Name')


# In[19]:

df1 = df1.filter(df1["FOLLOWER_COUNT_INCREASE"] > 0)


# In[38]:

df1.registerAsTable('train_tableFin')
df1 = sqlContext.sql('SELECT * FROM train_tableFin ORDER BY FOLLOWER_COUNT_INCREASE DESC')
df1.count()


# In[41]:

df1.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("FOLLOWER_COUNT_INCREASE.csv")


# In[28]:

sqlContext.sql('SELECT * FROM train_table1 ORDER BY Tweets_Count DESC').registerAsTable('table_Pop')


# In[36]:

pop_users_tweet = sqlContext.sql('SELECT User_Screen_Name, MAX(Tweets_Count) AS TWEET_COUNT FROM table_Pop GROUP BY User_Screen_Name ORDER BY TWEET_COUNT DESC')


# In[39]:

pop_users_tweet.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("TOP_USERS_TWEET_COUNT.csv")


# In[37]:

pop_users_tweet.show(15)

