# MongoDB_Student_Database

#Importing necessary packages..
  
    import pymongo
    import json
    from pprint import pprint


#Hosting MongoDB using Local server..
    
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    
#Calling the database, collection and stored in a variable 'mydb', 'mycol'..

    mydb = client["Students_Database"]
    mycol=mydb['Student_Master_Data']

#Reading the Json file and converting it into 'dict' format and inserted into the collection using FOR loop:

    with open('students.json', 'r') as file:
        try:
            for i in file:
                Temp_Data=json.loads(i)
                x=mycol.insert_one(Temp_Data)
        except pymongo.errors.DuplicateKeyError:
            pass

    for i in mycol.find():
        print(i)
