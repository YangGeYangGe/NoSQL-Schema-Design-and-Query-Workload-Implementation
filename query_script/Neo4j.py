from py2neo import Graph

def sq1(questionId = 7):
    return db.run("Match (u:User)-[r:own|isparent*1..2]-(p:Post{PostId:"+str(questionId)+"}) \
Return distinct u.CreationDate, u.DisplayName, u.UpVotes, u.DownVotes \
")

def sq2(TagName = "deep-learning"):
    return db.run("Match (p:Post)-[]->(t:Tag{TagName:\""+TagName+"\"}) \
Return p order by p.ViewCount Desc limit 1")

def aq1(l = ["neural-networks","neurons"]):
    return db.run("With "+ str(l) + " as tags \
unwind tags as tag \
Match (t:Tag{TagName:tag})<-[r1:haveTag]-(question:Post)-[r2:accept]->(answer:Post) \
with tag,min(duration.between(question.CreationDate,answer.CreationDate).seconds) as m \
Match (t:Tag)<-[r1:haveTag]-(question:Post)-[r2:accept]->(answer:Post) \
Where t.TagName =tag and duration.between(question.CreationDate,answer.CreationDate).seconds = m \
return tag,question.PostId \
")

def aq5(alpha = 10):
    return db.run("match ()<-[:accept]-(p:Post)-[:havevotes]-(v:Vote) \
where v.VoteTypeId = 2 \
with p, count(*) as coun \
with p.PostId as pid, coun \
where coun > "+str(alpha)+" with pid \
match (p1:Post{PostId:pid})-[:isparent]-(p:Post)-[:havevotes]-(v:Vote) \
where v.VoteTypeId = 2 \
with p1.PostId as question ,p.PostId as answer, count(*) as upvotes \
with question, max(upvotes) as max order by question \
match (p1:Post)-[:accept]->(p:Post)-[:havevotes]->(v:Vote) \
where v.VoteTypeId = 2 and p1.PostId =question \
with p1.PostId as question , count(*) as accepted, max order by question  \
where accepted <> max \
return question, accepted, max \
")

def aq6(userid = 4398):
    return db.run("Match (u:User) -[r1:own]->(p1:Post)-[r2:isparent*1..2]-(p2:Post)<-[r3:own]- (u2:User) \
Where u.UserId = "+str(userid)+" and u2.UserId <> "+str(userid)+" \
Return u2.UserId, count(*) as count \
Order by count desc \
limit 5 \
")

mypassword = "123123"
db = Graph("bolt://localhost:7687",password=mypassword)

print("for SQ1")
print(sq1(7).data())

print("for SQ2")
print(sq2("deep-learning").data())

print("for AQ1")
print(aq1(["neural-networks","neurons"]).data())

print("for AQ5")
print(aq5(10).data())

print("for AQ6")
print(aq6(4398).data())