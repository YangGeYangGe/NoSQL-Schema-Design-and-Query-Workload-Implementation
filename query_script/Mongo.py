import pymongo
import datetime

class QueryDemo():

    def __init__(self):
        pass

    def getConnection(self):
        client = pymongo.MongoClient(host='localhost', port=27017)
        db = client.A
        self.userscollection = db.get_collection('Users')
        self.postscollection = db.get_collection('Posts')


    def SQ1(self, questionid):
        self.getConnection()
        for item in self.postscollection.aggregate([{'$lookup':{
                                        'from':'Users',
                                        'localField':'OwnerUserId',
                                        'foreignField':'Id',
                                        'as':'user_detail'}},
                                        {'$match':{
                                                    '$or':[{'Id':questionid},{'ParentId':questionid}]}},
                                        {'$project':{
                                                    'subdocument':{'$arrayElemAt':['$user_detail',0]}}},
                                        {'$project':{
                                                    'Id':'$subdocument.Id',
                                                    'CreationDate':'$subdocument.CreationDate', 
                                                    'DisplayName':'$subdocument.DisplayName',
                                                    'UpVotes':'$subdocument.UpVotes',
                                                    'DownVotes':'$subdocument.DownVotes'}}
                                        ]):
            print(item)

    def SQ2(self, tag):
        self.getConnection()
        for item in self.postscollection.aggregate([
                                        {'$match':{
                                                    'Tags':tag}},
                                        {'$group':{
                                                    '_id':'$Id',
                                                    'view_sum':{'$sum':'$ViewCount'}}},
                                        {'$sort':{
                                                    'view_sum':-1}},
                                        {'$limit':1}
                                        ]):
            print(item)

    def AQ2(self, starttime, endtime):
        self.getConnection()
        for item in self.postscollection.aggregate([
                                        {'$match':{
                                                    'CreationDate':{'$gte':datetime.datetime.strptime(starttime, '%Y-%m-%dT%H:%M:%S'), '$lte':datetime.datetime.strptime(endtime, '%Y-%m-%dT%H:%M:%S')}}},
                                        {'$unwind':'$Tags'},
                                        {'$group':{
                                                    '_id':'$Tags', 
                                                    'User':{'$addToSet':'$OwnerUserId'}}},
                                        {'$project':{
                                                    'Tag':'$Tag', 
                                                    'User_number':{'$size':'$User'}}},
                                        {'$sort':{
                                                    'User_number':-1}},
                                        {'$limit':5}
                                        ]):
            print(item)

    def AQ3(self, tag):
        self.getConnection()
        for item in self.postscollection.aggregate([
                                        {'$match':{
                                                    'Tags':tag, 
                                                    'PostTypeId':1, 
                                                    'AcceptedUser_Id':{'$exists':True}}},
                                        {'$group':{
                                                    '_id':'$AcceptedUser_Id', 
                                                    'Question_detail':{
                                                                        '$push':{'Post_Id':'$Id',
                                                                        'Post_Title':'$Title'}}, 
                                                                        'count':{'$sum':1}}}, 
                                        {'$sort':{'count':-1}},
                                        {'$limit':1},
                                        ]):
            print(item)

    def AQ4 (self, User_Id, threshold, time):
        self.getConnection()
        tag_list = []
        for item in self.postscollection.aggregate([
                                        {'$match':{
                                                    'AcceptedUser_Id':User_Id}},
                                        {'$unwind':'$Tags'},
                                        {'$group':{
                                                    '_id':'$Tags', 
                                                    'count':{'$sum':1}}},
                                        {'$sort':{
                                                    'count':-1}},
                                        {'$match':{
                                                    'count':{'$gte':threshold}}},
                                        {'$project':{'_id':'$_id'}}

                                        ]):
            tag_list.append(item["_id"])

        for item in self.postscollection.aggregate([
                                        {'$match':{
                                                    'AcceptedUser_Id':{'$exists':False}, 
                                                    'PostTypeId':1, 
                                                    'CreationDate':{'$lt':datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S')}, 
                                                    'Tags':{'$in': tag_list}}},
                                        {'$sort':{'CreationDate':-1}},
                                        {'$limit':5},
                                        {'$project':{
                                                     'Id':'$Id', 
                                                     'Title':'$Title',
                                                     'CreationDate':'$CreationDate'}}
                                        ]):
            print(item)

qd = QueryDemo()

#query_script
print("for SQ1 Question Id 7")
qd.SQ1(7)#questionId
print("\n for SQ2 Tag name deep-learning")
qd.SQ2('deep-learning')#tagName
print("\n for AQ2 Tag name start time 2018-08-01T00:00:00 end time 2018-08-31T00:00:00")
qd.AQ2('2018-08-01T00:00:00', '2018-08-31T00:00:00')#startTime, endTime
print("\n for AQ3 Tag name deep-learning")
qd.AQ3('deep-learning')#tagName
print("\n for AQ4 AcceptedUser Id 4398 threshold 4 time 2018-08-30T00:00:00")
qd.AQ4(4398, 4, '2018-08-30T00:00:00')#AcceptedUser_Id, threshold, time
