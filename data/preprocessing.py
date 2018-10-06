import pandas as pd
from py2neo import Graph

# #for sq1
# a = db.run("Match (u:User)-[r:own]->(p:Post) \
# Where p.PostId = 7 or p.ParentId = 7 \
# Return u.CreationDate, u.DisplayName, u.UpVotes, u.DownVotes \
# ")
# a.data()
def change_to_csv():
    tsv_file='Posts.tsv'
    csv_table=pd.read_table(tsv_file,sep='\t')
    csv_table.to_csv('Posts1.csv',index=False)

    tsv_file='Tags.tsv'
    csv_table=pd.read_table(tsv_file,sep='\t')
    csv_table.to_csv('Tags1.csv',index=False)

    tsv_file='Users.tsv'
    csv_table=pd.read_table(tsv_file,sep='\t')
    csv_table.to_csv('Users1.csv',index=False)

    tsv_file='Votes.tsv'
    csv_table=pd.read_table(tsv_file,sep='\t')
    csv_table.to_csv('Votes1.csv',index=False)

def load_data():
    db.run("LOAD CSV WITH HEADERS FROM \"file:///Votes.csv\" AS Votes create (a1:Vote {\
VoteId:Votes.Id, \
PostId: toInt(Votes.PostId), \
VoteTypeId: toInt(Votes.VoteTypeId), \
UserId: Votes.UserId}) \
")

    db.run("LOAD CSV WITH HEADERS FROM \"file:///Users.csv\" AS \
    Users create (a1:User {UserId:toInt(Users.Id), \
    CreationDate: Users.CreationDate, \
    DisplayName: Users.DisplayName, \
    UpVotes: Users.UpVotes, \
    DownVotes: Users.DownVotes})")

    db.run("LOAD CSV WITH HEADERS FROM \"file:///Posts.csv\" AS Posts create (a1:Post {PostId:toInt(Posts.Id), \
PostTypeId: toInt(Posts.PostTypeId), \
AcceptedAnswerId: toInt(Posts.AcceptedAnswerId), \
CreationDate: Posts.CreationDate, \
ViewCount: toInt(Posts.ViewCount), \
OwnerUserId: toInt(Posts.OwnerUserId), \
Title: Posts.Title, \
Tags: split(Posts.Tags,\",\"), \
AnswerCount: toInt(Posts.AnswerCount), \
ParentId: toInt(Posts.ParentId) })")

    db.run("LOAD CSV WITH HEADERS FROM \"file:///Tags.csv\" AS Tags create (a1:Tag { \
TagId:Tags.Id, \
TagName: Tags.TagName,\
Count: Tags.Count})")

    db.run("Match (p:Post) \
SET p.CreationDate = datetime(p.CreationDate) \
")

def create_relationship():
    db.run("Match (u:User), (p:Post) \
Where u.UserId = p.OwnerUserId \
Create (u)-[r:own]->(p) \
")

    db.run("Match (p1:Post), (p2:Post) \
Where p1.PostId = p2.ParentId \
Create (p1)-[r:isparent]->(p2) \
")
    db.run("Match (p1:Post), (p2:Post) \
Where p1.PostId = p2.AcceptedAnswerId \
Create (p2)-[r:accept]->(p1) \
")

    db.run("Match (p:Post), (t:Tag) \
Where t.TagName in p.Tags \
Create (p)-[r:haveTag]->(t) \
")
    db.run("Match (p:Post), (v:Vote) \
Where p.PostId = v.PostId \
Create (p)-[r:havevotes]->(v) \
")

def create_index():
    db.run("create index on :Tag(TagName)")
    db.run("create index on :Post(PostId)")
    db.run("create index on :User(UserId)")
    db.run("create index on :Vote(VoteTypeId)")

def delete_all():
    db.run("Match (n) Detach delete n")
    
# change_to_csv()

mypassword = "123123"
db = Graph("bolt://localhost:7687",password=mypassword)
delete_all()
load_data()
create_relationship()
create_index()