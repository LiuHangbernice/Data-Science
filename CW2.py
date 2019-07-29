#!/usr/bin/env python
# coding: utf-8

# # Coursework 2: Data Processing
# 
# ## Task 1
# This coursework will assess your understanding of using NoSQL to store and retrieve data.  You will perform operations on data from the Enron email dataset in a MongoDB database, and write a report detailing the suitability of different types of databases for data science applications.  You will be required to run code to answer the given questions in the Jupyter notebook provided, and write a report describing alternative approaches to using MongoDB.
# 
# Download the JSON version of the Enron data (using the “Download as zip” to download the data file from http://edshare.soton.ac.uk/19548/, the file is about 380MB) and import into a collection called messages in a database called enron.  You do not need to set up any authentication.  In the Jupyter notebook provided, perform the following tasks, using the Python PyMongo library.
# 
# Answers should be efficient in terms of speed.  Answers which are less efficient will not get full marks.

# In[1]:



#mongoDB 4.0 

import pymongo
from pymongo import MongoClient
from datetime import datetime
from pprint import pprint

from pymongo import MongoClient


# In[3]:


get_ipython().run_cell_magic('cmd', '', '\nmongoimport --db enron --collection messages --drop --file ./messages.json')


# ### 1)
# Write a function which returns a MongoDB connection object to the "messages" collection. [4 points] 

# In[2]:


def get_collection():
    """
    Connects to the server, and returns a collection object
    of the `messages` collection in the `enron` database
    """
    
    # YOUR CODE HERE
    client = MongoClient('mongodb://localhost:27017')
    client.list_database_names()

    db = client.enron
    collection = db.messages
    return collection

get_collection()


# ### 2)
# 
# Write a function which returns the amount of emails in the messages collection in total. [4 points] 

# In[3]:


import sys
def get_amount_of_messages(collection):
    """
    :param collection A PyMongo collection object
    :return the amount of documents in the collection
    """    
    print(collection.count())
#     print(collection.count_documents({'mailbox': 'bass-e'}))
#     print(collection.distinct('mailbox'))

    
    return collection.count()
collection = get_collection()
get_amount_of_messages(collection) 


# ### 3) 
# 
# Write a function which returns each person who was BCCed on an email.  Include each person only once, and display only their name according to the X-To header. [4 points] 
# 
# 

# In[86]:


def get_bcced_people(collection):
    """
    :param collection A PyMongo collection object
    :return the names of the people who have received an email by BCC
    """    
    # YOUR CODE HERE
    b = {}
    bcc_list = collection.distinct('headers.Bcc')
    name_list = []
    for i in bcc_list:
        if ',' in i:
            i = i.split(',')
        if len(i) == 1:
            name = i.split('@')[0]
            print(i)
            if name == '':
                continue
            if b.get(name):
                continue

            else:
                b[name] = '1'
                name_list.append(name)
        else:
            for p in i:  

                if '@' in p:
                    name = p.split('@')[0]
                    name = name.strip()
                    if '\r\n' in name:
                        name = name.split('\r\n')[1]
                    if name == '':
                        continue
                    if b.get(name):
                        continue
                    else:
                        b[name] = '1'
                        name_list.append(name)

    print(len(name_list))
    return name_list

collection = get_collection()

get_bcced_people(collection) 


# ### 4)
# 
# Write a function with parameter subject, which gets all emails in a thread with that parameter, and orders them by date (ascending). “An email thread is an email message that includes a running list of all the succeeding replies starting with the original email.”, check for detail descriptions at https://www.techopedia.com/definition/1503/email-thread [4 points]

# In[101]:


from pprint import pprint
def get_emails_in_thread(collection, subject):
    """
    :param collection A PyMongo collection object
    :return All emails in the thread with that subject
    """    
    # YOUR CODE HERE    
    sort = [('headers.Date', pymongo.ASCENDING)]
    cursor = collection.find({'headers.Subject': subject}, sort = sort)
    for c in cursor:
        pprint(c)
    
collection = get_collection()
subject = 'hey'
get_emails_in_thread(collection,subject) 


# ### 5)
# 
# Write a function which returns the percentage of emails sent on a weekend (i.e., Saturday and Sunday) as a `float` between 0 and 1. [6 points]

# In[ ]:


from __future__ import division

def get_percentage_sent_on_weekend(collection):
    """
    :param collection A PyMongo collection object
    :return A float between 0 and 1
    """    
    # YOUR CODE HERE
    count = collection.count_documents({'headers.Date': {"$regex": '^Sat|Sun'}})
    rest = collection.count_documents({'headers.Date': {"$regex": '^Mon|Tue|Fri|Wed|Thu'}})
    total = collection.count_documents({'headers.Date': {"$regex": '^'}})
    percentageofsentmail = round(count/total,2)
    return percentageofsentmail
    
collection = get_collection()
get_percentage_sent_on_weekend(collection)


# In[40]:





# ### 6)
# 
# Write a function with parameter limit. The function should return for each email account: the number of emails sent, the number of emails received, and the total number of emails (sent and received). Use the following format: [{"contact": "michael.simmons@enron.com", "from": 42, "to": 92, "total": 134}] and the information contained in the To, From, and Cc headers. Sort the output in descending order by the total number of emails. Use the parameter limit to specify the number of results to be returned. If limit is null, the function should return all results. If limit is higher than null, the function should return the number of results specified as limit. limit cannot take negative values. [10 points]

# In[75]:



def get_emails_between_contacts(collection, limit):
    """
    Shows the communications between contacts
    Sort by the descending order of total emails using the To, From, and Cc headers.
    :param `collection` A PyMongo collection object    
    :param `limit` An integer specifying the amount to display, or
    if null will display all outputs
    :return A list of objects of the form:
    [{
        'contact': <<Another email address>>
        'from': 
        'to': 
        'total': 
    },{.....}]
    """    
    cursor = collection.aggregate([{
        '$project': {"headers.From" : 1,
                     "To":{
                         "$split":["$headers.To",","],
                         
                     },
                     "CC":{
                         "$split":["$headers.Cc",","],
                    },
                     '_id':0}},
        {'$unwind':"$CC"
        },
        {'$unwind':"$To"
        },
        {
        '$group':{
            "_id":"$CC","cc_amonut":{"$sum":1},
    }}])
    cursor_1 = collection.aggregate([{
        '$project': {"headers.From" : 1,
                     "To":{
                         "$split":["$headers.To",","],   
                     },
                     '_id':0}},
        {'$unwind':"$To"
        },
        {
        '$group':{
            "_id":"$To","to_amonut":{'$sum':1},

    }}])
    cursor_2 = collection.aggregate([{
        '$project': {"headers.From" : 1,
                     
                     '_id':0}},
        {
        '$group':{
            "_id":"$headers.From","from_amonut":{'$sum':1},

    }}])
    
    result = []
    contact = {}
    
    for c in cursor:
        temp = {}
        key = remove_sth(c['_id'])
        value = c['cc_amonut']
        temp["cc_amount"] = value
        contact[key] = temp

    for c in cursor_1:
        temp = {}
        key = remove_sth(c['_id'])
        value = c['to_amonut']
        temp["to_amount"] = value
        if key in contact.keys():
            temp_value = contact[key]  
            if temp_value.get('to_amount'):
                continue
            contact[key] = {}
            contact[key]["cc_amount"] = temp_value
            contact[key]["to_amount"] = value   
        else:
            contact[key] = {}
            contact[key]["cc_amount"] = 0
            contact[key]["to_amount"] = value
    print(contact)
    for c in cursor_2:
        temp = {}
        key = remove_sth(c['_id'])
        value = c['from_amonut']
        temp["from_amonut"] = value
        if key in contact.keys():
            temp_value1 = contact[key]["cc_amount"]
            
            if contact[key].get("to_amount"):
                temp_value2 = contact[key]["to_amount"]
            else:
                temp_value2 = 0
            contact[key] = {}
            contact[key]["cc_amount"] = temp_value1
            contact[key]["to_amount"] = temp_value2  
            contact[key]["from_amount"] = value 
        else:
            contact[key] = {}
            contact[key]["cc_amount"] = 0
            contact[key]["to_amount"] = 0
            contact[key]["from_amonut"] = value             

    for key in contact.keys():
        
        p = {}
        p['contact'] = key
        v1 = 0
        v2 = 0
        v3 = contact[key]["cc_amount"]
        
        if contact[key].get("from_amonut"):
            v1 = contact[key]["from_amonut"]
        
        if contact[key].get("to_amonut"):
            v2 = contact[key]["to_amount"]
        p['from'] = v1

        if type(v3) is dict:
            continue
        else:
            p['to'] = v2
            p['total'] = int(v3) + int(v2)
        print(contact[key])
        print(p)
        result.append(p)
            
    print(result)
    
def remove_sth(key):
    if key != None:
        key= key.strip() 
        key= key.replace('/r/n','')
        key= key.replace('<','')
        key= key.replace('>','')
    return key  

limit = 5
collection = get_collection()
get_emails_between_contacts(collection, limit)


# (### 7)
# Write a function to find out the number of senders who were also direct receivers. Direct receiver means the email is sent to the person directly, not via cc or bcc. [4 points]

# In[9]:


def get_from_to_people(collection):
    """
    :param collection A PyMongo collection object
    :return the NUMBER of the people who have sent emails and received emails as direct receivers.
    """    
    receivers = collection.aggregate([{
        '$project': {"headers.From" : 1,
                     "To":{
                         "$split":["$headers.To",","],
                         
                     },
                     '_id':0}},
        {'$unwind':"$To"
        },
        {
        '$group':{
            "_id":"$headers.From",'cc_amonut':{'$sum':1},
    }},])
    sender = collection.aggregate([{
        '$project': {"headers.From" : 1,
                     "To":{
                         "$split":["$headers.To",","],
                         
                     },
                     '_id':0}},
               {'$unwind':"$To"
        },
        {
        '$group':{
            "_id":"$To",'cc_amonut':{'$sum':1},
    }}, 
        ])
    
    count = 0
    c = {}
    
    for i in sender:
        key= remove_sth(i['_id'])
        c[key] = 0
    
    
    for p in receivers:
        key = remove_sth(p['_id'])
        if c.get(key) != None: 
            c[key] =c[key] + 1
    for key in c.keys():
        if c[key] != 0:
            count = count +1
    print(count)  

    
    
def remove_sth(key):
    key= key.strip() 
    key= key.replace('/r/n','')
    key= key.replace('<','')
    key= key.replace('>','')
    key= key.replace('email ','')
    return key

collection = get_collection()
get_from_to_people(collection)


# ### 8)
# Write a function with parameters start_date and end_date, which returns the number of email messages that have been sent between those specified dates, including start_date and end_date [4 points] 

# In[87]:


import datetime



def get_emails_between_dates(collection,start_date,end_date):
    """
    :param collection A PyMongo collection object
    :return All emails between the specified start_date and end_date
    """    
    # YOUR CODE HERE
    



    a = collection.aggregate([{"$project": {
           #Tue, 14 Nov 2000 02:40:00 -0800 (PST)
          "date": {
            "$substr": ["$headers.Date", 5,11],                  
             },
#          "month": {
#              "$substrCP": ["$headers.Date",7,4],  
#          }
        "body":1,
        "headers.Date":1,
          }
       },{
        "$addFields":{
            "day":{
#             "$toDate":"$date"
            "$convert": 
                { "input": "$date", 
                 "to": "date", 
                 "onError": "1990-0-0" }
            }
         }}, {"$match":{
            "day":{"$gte":start_date, "$lt":end_date}
        }}, ])
    
    result = []
    for i in a:
        result.append(i)
    
    print(result)
    return result

collection = get_collection()
start_date = datetime.datetime(2012, 2, 2, 6, 35, 6, 764)
end_date = datetime.datetime(2022, 2, 2, 6, 55, 3, 381)
get_emails_between_dates(collection,start_date,end_date)


# ## Task 2
# This task will assess your ability to use the Hadoop Streaming API and MapReduce to process data. For each of the questions below, you are expected to write two python scripts, one for the Map phase and one for the Reduce phase. You are also expected to provide the correct parameters to the `hadoop` command to run the MapReduce process. Write down your answers in the specified cells below.
# 
# To get started, you need to download and unzip the YouTube dataset (available at http://edshare.soton.ac.uk/19547/) onto the machine where you have Hadoop installed (this should be the virtual machine provided).
# 
# To help you, `%%writefile` has been added to the top of the cells, automatically writing them to "mapper.py" and "reducer.py" respectively when the cells are run.

# ### 1) 
# Using Youtube01-Psy.csv, find the hourly interval in which most spam was sent. The output should be in the form of a single key-value pair, where the value is a datetime at the start of the hour with the highest number of spam comments. [9 points]

# In[ ]:


get_ipython().run_cell_magic('bash', '', '\nwget https://archive.ics.uci.edu/ml/machine-learning-databases/00380/YouTube-Spam-Collection-v1.zip \\\n-O YouTube-Spam-Collection-v1.zip\n\nunzip -o YouTube-Spam-Collection-v1.zip\n\nls -lh *.csv')


# In[ ]:


get_ipython().run_cell_magic('writefile', 'mapper.py', '#!/usr/bin/env python2.7 \n# MAPPER\n\nimport csv\nimport sys\nimport re\n\n\nlines = sys.stdin.readlines()\ncsvreader = csv.reader(lines)\n\ndates = [row[2] for row in csvreader]\n\n\nfor date in dates:\n\n    token = date.split(":")[0]\n    print(token + "\\t1")\n\n        ')


# In[ ]:


get_ipython().run_cell_magic('writefile', 'reducer.py', '#!/usr/bin/env python2.7 \n#REDUCER\n\nimport sys\nfrom collections import defaultdict\n\ninput_pairs = sys.stdin.readlines()\n\naccumulator = defaultdict(lambda: 0)\n\nfor row in input_pairs:\n    key_value_pair = row.split("\\t", 1)\n\n    if len(key_value_pair) != 2:\n        continue\n        \n    word = key_value_pair[0]\n    count = int(key_value_pair[1].strip())\n    accumulator[word] = accumulator[word] + count\n    \nfor (key, value) in accumulator.items():\n    print(key + "\\t" + str(value))\n    \n%%bash\nchmod a+x mapper.py reducer.py\ncat Youtube01-Psy.csv | ./mapper.py | ./reducer.py | sort')


# In[ ]:


get_ipython().run_cell_magic('bash', '', '# Clear output\nrm -rf output\n\n# Make sure hadoop is in standalone mode\nhadoop-standalone-mode.sh\n\nhadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \\\n-files mapper.py,reducer.py \\\n-input Youtube01-Psy.csv  \\\n-mapper ./mapper.py \\\n-reducer ./reducer.py \\\n-output output')


# In[ ]:


#Expected key-value output format:
#hour_with_most_spam	"2013-11-10T10:00:00"

#Additional key-value pairs are acceptable, as long as the hour_with_most_spam pair is correct.


# ### 2) 
# Find all comments associated with a username (the AUTHOR field). Return a JSON array of all comments associated with that username. (This should use the data from all 5 data files: Psy, KatyPerry, LMFAO, Eminem, Shakira) [11 points]

# In[ ]:


get_ipython().run_cell_magic('writefile', 'mappertest.py', '#!/usr/bin/env python2.7\n#MAPPER\n\nimport csv\nimport sys\nimport re\n\nlines = sys.stdin.readlines()\ncsvreader = csv.reader(lines)\n    \ncomments = [[row[1],row[3]] for row in csvreader]\n\nfor comment in comments:\n\n    author = re.split("\\s",comment[0])[0]\n\n    content = comment[1]\n    token = author + "\\t" + content \n    print(token)\n        ')


# In[ ]:


get_ipython().run_cell_magic('writefile', 'reducertest.py', '#!/usr/bin/env python2.7\n#REDUCER\n\nimport sys\nfrom collections import defaultdict\n\n\n# input_pairs = [\n# \'Julius\tHuh, anyway check out this you[tube] channel: kobyoshi02\',\n# "adam\tHey guys check out my new channel and our first vid THIS IS US THE  MONKEYS!!! I\'m the monkey in the white shirt,please leave a like comment  and please subscribe!!!!",\n# \'Evgeny\tjust for test I have to say murdev.com\',\n# \'ElNino\tme shaking my sexy ass on my channel enjoy ^_^ \',\n# \'GsMega\twatch?v=vtaRGgvGtWQ   Check this out\',\n# \'Jason\tHey, check out my new website!! This site is about kids stuff. kidsmediausa  . com\',\n# \'ferleck\tSubscribe to my channel\',\n# \'ferleck\tto my channel\',\n# \'Bob\ti turned it on mute as soon is i came on i just wanted to check the  views...\',\n# ]\ninput_pairs = sys.stdin.readlines()\naccumulator = {}\n\nfor row in input_pairs:\n\n    key_value_pair = row.split("\\t")\n    \n    if len(key_value_pair) != 2:\n        continue\n\n    key = key_value_pair[0]\n    content = key_value_pair[1] \n    \n    if accumulator.get(key):     \n        old_comment_list = accumulator.get(key)   \n        old_comment_list.append(content)  \n        new_comment_list = old_comment_list\n    else:\n        new_comment_list = [content]\n        #print(new_comment_list)\n    accumulator[key] = new_comment_list\n    \nfor (key, value) in accumulator.items():\n    print(key + "\\t" + str(value))\n\n    \n%%bash\n\nchmod a+x mappertest.py reducertest.py\ncat Youtube04-Eminem.csv | ./mappertest.py | ./reducertest.py | sort')


# In[ ]:


get_ipython().run_cell_magic('bash', '', '\nhadoop-standalone-mode.sh\n\nhadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \\\n-files mappertest.py,reducertest.py \\\n-input Youtube01-Psy.csv,Youtube02-KatyPerry.csv,Youtube05-Shakira.csv,Youtube03-LMFAO.csv,Youtube04-Eminem.csv   \\\n-mapper ./mappertest.py \\\n-reducer ./reducertest.py \\\n-output out')


# In[ ]:


#Expected key-value output format:
#John Smith	["Comment 1", "Comment 2", "Comment 3", "etc."]
#Jane Doe	["Comment 1", "Comment 2", "Comment 3", "etc."]


# In[ ]:




