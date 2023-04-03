# MongoDB_Student_Database

#Importing necessary packages..
  import pymongo
  import json
  from pprint import pprint


#Hosting MongoDB using Local server..
client = pymongo.MongoClient("mongodb://localhost:27017/")
