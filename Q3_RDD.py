
# coding: utf-8

# In[1]:

lines = sc.textFile("hdfs:/user/cloudera/INPUT.txt")
lines.take(5)


# In[2]:

def tupMaker(inp):
    t = inp.split(',')
    return (t[0], t[1])


# In[16]:

words = lines.map(tupMaker)
words.sortByKey()
words.take(10)


# In[40]:

w = words.distinct()
w.take(5)


# In[5]:

words_hashtags = words.map(lambda v: (v[0], 1))
words_hashtags.take(10)


# In[ ]:

#newRDD.takeOrdered(2, key=lambda x: -x[2])


# In[63]:

words = words.filter(lambda a: len(a[0])>=1)
xy = words.groupByKey().map(lambda x : (x[0], list(x[1])))
xy = xy.map(lambda a: (a[0], list(set(a[1]))))
xy.take(10)


# In[ ]:




# In[19]:

counts = words_hashtags.reduceByKey(lambda a,b: (a+b))
counts.take(5)
counts.count()


# In[38]:

res = counts.filter(lambda a: len(a[0])>=1)
bSorted = res.sortBy(lambda a: a[1])
bSorted.take(10)


# In[59]:




# In[64]:

xy.coalesce(1).saveAsTextFile('hdfs:/user/cloudera/HT_TZ.txt')


# In[47]:

bSorted.coalesce(1).saveAsTextFile('hdfs:/user/cloudera/OUTPUT_WordCount.txt')


# In[23]:




# In[17]:
res.foreach(comp)