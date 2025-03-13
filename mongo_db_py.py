import pandas 
import pymongo 
import csv
import neo4j

m=[]
m1=[]
m2=[]
m3=[]
n=[]
n1=[]
n2=[]
n3=[]




connect = pymongo.MongoClient("mongodb://localhost:27017")
database = connect["testdb"]



for j in ('1','2','3','4'):
    xm=[]
    xm1=[]
    xm2=[]
    xm3=[]
    dbread = pandas.read_csv("C:/Users/vinay/Desktop/datacsv/data"+j+".csv")
    dbread = dbread.to_dict(orient="records")
    service_coll = database['service']
    hospital_coll = database['hospital']
    person_coll = database['person']
    specialization_coll = database['specialization']
    for row in dbread:
        service_coll.insert_one({'No_of_beds':row['No_of_beds'],'hospital_id':row['hospital_id']})
        hospital_coll.insert_one({'Hospital_names':row['Hospital_names'],'hospital_id':row['hospital_id'],
                                  'country':row['country'],'city':row['city'],'reg_date':row['reg_date'],'emergency_no':row['emergency_no']})
        person_coll.insert_one({'first_name':row['first_name'],'Last_name':row['Last_name'],'email':row['email'],'hospital_id':row['hospital_id']})
        specialization_coll.insert_one({'specialization':row['specialization'],'hospital_id':row['hospital_id'],'activity_area':row['activity_area']})
        
    for i in range(0,31):
        mydoc = person_coll.find({}).explain()
        m = mydoc['executionStats']['executionTimeMillis']
        xm.append(m)
        mydoc = hospital_coll.find({"country":"italy"}).explain()
        m1= mydoc['executionStats']['executionTimeMillis']
        xm1.append(m1)
        query3 = specialization_coll.find({'specialization':'gynecology'})
        query3_time = query3.explain()
        query3_hospital_id = []
        for x in query3:
            query3_hospital_id.append(x['hospital_id'])
        y= hospital_coll.find({'hospital_id':{'$in':query3_hospital_id}},{'Hospital_name':1,'hospital_id':1,'_id':0}).explain() 
        m2 = query3_time['executionStats']['executionTimeMillis']
        xm2.append(m2)
        query4_codes = service_coll.find({'No_of_beds':{'$gt':500,'$lt' :1200}})
        query4_time = query4_codes.explain()
        query4_hospital_id =[]            
        for X in query4_codes:
            query4_hospital_id.append(X['hospital_id'])
        query4_person = person_coll.find({'hospital_id':{'$in':query4_hospital_id}},{'first_name':1,'Last_name':1,'city':1,'_id':0}).explain()
        query4_hospital = hospital_coll.find({'hospital_id':{'$in':query4_hospital_id}},{'country':1,'city':1,'_id':0}).explain()
        m3 = query4_time['executionStats']['executionTimeMillis']
        xm3.append(m3)
    
    print(hospital_coll.find({"country":"italy"})) 

    print(xm,xm1,xm2,xm3)
    

    service_coll.drop()
    hospital_coll.drop()
    person_coll.drop()
    specialization_coll.drop()
    