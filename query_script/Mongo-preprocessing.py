import pymongo

class QueryDemo():

    def __init__(self):
        pass

    def getConnection(self):
        client = pymongo.MongoClient(host='localhost', port=27017)
        db = client.A
        self.userscollection = db.get_collection('Users')
        self.postscollection = db.get_collection('Posts')

    def Users_preprocessing(self):
        self.getConnection()
        self.userscollection.aggregate([{'$project':{
                                                        'Id':1,
                                                        'CreationDate':1,
                                                        'DisplayName':1,
                                                        'UpVotes':1,
                                                        'DownVotes':1}},
                                        {'$out':'Users'}
                            ])

    def Posts_preprocessing(self):
        self.getConnection()
        self.postscollection.aggregate([{'$lookup':{
                                                        'from':'Posts',
                                                        'localField':'ParentId',
                                                        'foreignField':'Id',
                                                        'as':'Question_detail'}},
                                        {'$unwind':{
                                                    'path':"$Question_detail",
                                                    'preserveNullAndEmptyArrays':True}},
                                        {'$project':{
                                                    'Id':1,
                                                    'PostTypeId':1, 
                                                    'AcceptedAnswerId':1, 
                                                    'CreationDate':{'$dateFromString':{'dateString':'$CreationDate'}}, 
                                                    'ViewCount':1, 
                                                    'OwnerUserId':1, 
                                                    'Title':1,
                                                    'Tags':{'$cond':{'if':{'$eq':['$PostTypeId',1]}, 'then':'$Tags', 'else':'$Question_detail.Tags'}},
                                                    'ParentId':1}},
                                        {'$lookup':{'from':'Posts',
                                                    'localField':'AcceptedAnswerId',
                                                    'foreignField':'Id',
                                                    'as':'Answerpost_detail'}},
                                        {'$project':{'Id':1,
                                                     'PostTypeId':1, 
                                                     'AcceptedAnswerId':1, 
                                                     'CreationDate':1, 
                                                     'ViewCount':1, 
                                                     'OwnerUserId':1, 
                                                     'Title':1,
                                                     'Tags':{'$split':['$Tags',',']}, 
                                                     'subdocument':{"$arrayElemAt":["$Answerpost_detail",0]},
                                                     'ParentId':1}},
                                        {'$project':{
                                                     'Id':1,
                                                     'PostTypeId':1, 
                                                     'AcceptedAnswerId':1, 
                                                     'CreationDate':1, 
                                                     'ViewCount':1, 
                                                     'OwnerUserId':1, 
                                                     'Title':1,
                                                     'Tags':1,
                                                     'ParentId':1, 
                                                     'AcceptedUser_Id':'$subdocument.OwnerUserId'}},
                                        {'$out':'Posts'}
                                        ])



qd = QueryDemo()

#preprocessing
qd.Users_preprocessing()
qd.Posts_preprocessing()