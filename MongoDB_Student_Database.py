#Importing necessary packages..
import pymongo
import json
from pprint import pprint

#Hosting MongoDB using Local server..
client = pymongo.MongoClient("mongodb://localhost:27017/")

#Calling the database, collection and stored in a variable 'mydb', 'mycol'
mydb = client["Students_Database"]
mycol=mydb['Student_Master_Data']

#Reading the Json string file and converting it into 'dict' format and inserted into the collection using FOR loop:
with open('students.json', 'r') as file:
    try:
        for i in file:
            Temp_Data=json.loads(i)
            x=mycol.insert_one(Temp_Data)
    except pymongo.errors.DuplicateKeyError:
        pass

for i in mycol.find():
    print(i)


#Task1 - Find the student name who scored maximum scores in all (exam, quiz and homework)?

stage1= {'$addFields':{"Total_Score":{"$sum":{"$sum":["$scores.score"]}}}}
stage2={'$sort':{'Total_Score':-1}}
stage3={'$limit':1}
for i in mycol.aggregate([stage1,stage2,stage3]):
    print(i)


#Task2 -  Find students who scored below average in the exam and pass mark is 40%?

stage1={'$unwind':'$scores'}
stage2={'$match':{'scores.type':'exam'}}
stage3={'$group':{'_id':'$scores.type','Avg_Score':{'$avg':'$scores.score'}}}
for i in mycol.aggregate([stage1,stage2,stage3]):
    x=i

stage5={'$match':{'$and':[{'scores.type':'exam'},{'$and':[{'scores.score':{'$lt':x['Avg_Score']}},{'scores.score':{'$gte':40}}]}]}}
stage6={'$sort':{'name':1}}
stage7={'$project':{'_id':0}}
for i in mycol.aggregate([stage1,stage5,stage6,stage7]):
    print(i)


#Task3 - Find students who scored below pass mark and assigned them as fail, and above pass mark as pass in all the categories.

for i in mycol.find():
    if (i['scores'][0]['score'])>=40 and (i['scores'][1]['score'])>=40 and (i['scores'][2]['score'])>=40:
        i['status']='Pass'
        print(i)
    else:
        i['status']='Fail'
        print(i)


#Task4 - Find the total and average of the exam, quiz and homework and store them in a separate collection.

stage1={'$unwind':'$scores'}
stage2={'$group':{'_id':'$scores.type','Total_Score':{'$sum':'$scores.score'},'Average_Score':{'$avg':'$scores.score'}}}
stage3={'$out':'Total_&_Avg_Score'}
for i in mycol.aggregate([stage1,stage2,stage3]):
    print(i)

#To check new created collections,
mycol_total_avg_score = mydb['Total_&_Avg_Score']
for i in mycol_total_avg_score.find():
    print(i)


#Task5 - Create a new collection which consists of students who scored below average and above 40% in all the categories.

stage1={'$unwind':'$scores'}
stage2={'$group':{'_id':'$scores.type','Avg_Score':{'$avg':'$scores.score'}}}
avg_score = []
for i in mycol.aggregate([stage1,stage2]):
    avg_score.append(i['Avg_Score'])

query = {'$and':[{'$and':[{'scores.0.score':{'$gte':40}},{'scores.0.score':{'$lt':avg_score[0]}}]},
                 {'$and':[{'scores.1.score':{'$gte':40}},{'scores.1.score':{'$lt':avg_score[2]}}]},
                 {'$and':[{'scores.2.score':{'$gte':40}},{'scores.2.score':{'$lt':avg_score[1]}}]}]}

for i in mycol.find(query):
    print(i)
    mycol_avg_all_categories = mydb['avg_marks_all_categories']
    mycol_avg_all_categories.insert_one(i)

#To check new created collections,
mycol_avg_all_categories = mydb['avg_marks_all_categories']
for i in mycol_avg_all_categories.find():
    print(i)


#Task6 - Create a new collection which consists of students who scored below the fail mark in all the categories.

stage1={'$match':{'$and':[{'scores.0.score':{'$lt':40}},{'scores.1.score':{'$lt':40}},{'scores.2.score':{'$lt':40}}]}}
stage2={'$out':'Fail_in_All_Categories'}
for i in mycol.aggregate([stage1,stage2]):
    print(i)

#To check new created collections,
mycol_fail_all_categories = mydb['Fail_in_All_Categories']
for i in mycol_fail_all_categories.find():
    print(i)


#Task7 - Create a new collection which consists of students who scored above pass mark in all the categories.

stage1={'$match':{'$and':[{'scores.0.score':{'$gte':40}},{'scores.1.score':{'$gte':40}},{'scores.2.score':{'$gte':40}}]}}
stage2={'$out':'Pass_in_All_Categories'}
for i in mycol.aggregate([stage1,stage2]):
    print(i)

#To check new created collections,
mycol_pass_all_categories = mydb['Pass_in_All_Categories']
for i in mycol_pass_all_categories.find():
    print(i)
