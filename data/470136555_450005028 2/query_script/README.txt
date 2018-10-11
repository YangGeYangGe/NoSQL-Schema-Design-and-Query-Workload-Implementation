Software: 
    MongoDB, Robo 3T; Neo4j Desktop 1.1.10; python3
Python3 Libraries:
    For MongoDB:
        pymongo,
	datetime

    For Neo4j:
        py2neo,
        pandas

How to use for MongoDB:
    Mongo.py
    Before running the python files, you may need to change “port = 27017” to your port
    and change database name in Mongo.py getConnection() function.
    
    For preprocessing, place tocsv.py and data in same location, run change_to_csv() 
    function in tocsv.py to change the data files to csv files. 

    After change to csv files, please go to the data path in terminal and input ‘mongoimport --db A --collection Posts --type csv --file Posts.csv --headerline’ this command in terminal to import Posts data to MongoDB and input ‘mongoimport --db A --collection Users --type csv --file Users.csv --headerline’ this command in terminal to import Users data to MongoDB.

    And in Mongo.py just run qd.Users_preprocessing() and qd.Posts_preprocessing()
    these two function to change the MongoDB schema.

    Just running SQ1(), SQ2(), AQ2(), AQ3() and AQ4() function to get query results.

    You can also run the Mongo shell in Mongo.py in MongoDB desktop application for
    both preprocessing and query.
    

How to use for Neo4j:
    Before running the python files, you may need to change "bolt://localhost:7687" to 
    other url and change the password "123123" inside two files.

    For preprocessing, place Neo4j-preprocessing.py，tocsv.py and data in same location,
    run the function change_to_csv() in tocsv.py to change the data files to csv files.
    Place the csv files to the place where the Neo4j can find them. 
    Run other part of the Neo4j-preprocessing.py file to finish preprocessing.

    Run Neo4j.py to get query results.
   
    You can also run the Cypher code in two python files directly in Neo4j 
    desktop application for both preprocessing and query.