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


connect = neo4j.GraphDatabase.driver("neo4j://localhost:7687",auth=('neo4j','password'))
database = connect.session()


for i in ('1','2','3','4'):
    xn=[]
    xn1=[]
    xn2=[]
    xn3=[]

    database.run("create constraint hospital_ids_db for (c:hospital) require c.hospital_id is unique")
    database.run("create constraint person_ids_db for (c:person) require c.email is unique")
    database.run("create constraint services_ids_db for (c:services) require c.no_of_beds is unique")
    database.run("create constraint specialization_ids_db for (c:specialization) require c.specialization_info is unique")
    
    '''load csv with headers from 'file:///datacsv/data"+i+".csv' as line FIELDTERMINATOR',' with line create (b:hospital{hospital_id: tointeger(line.hospital_id)}) merge (c:person{email: (line.email)}) merge (d:services{no_of_beds:tointeger(line.No_of_beds)}) merge (e:specialization{specialization:(line.specialization)}) set b.name = line.Hospital_names, b.country = line.country, b.city = tointeger(line.city) , b.reg_date=line.reg_date, c.first_name = line.first_name, c.Last_name = line.Last_name, e.specialization = line.activity_area'''

    database.run("load csv with headers from 'file:///datacsv/data" +i+".csv' as line FIELDTERMINATOR',' with line create (b:hospital{hospital_id: tointeger(line.hospital_id)}) merge (c:person{email: (line.email)}) merge (d:services{no_of_beds:tointeger(line.No_of_beds)}) merge (e:specialization {specialization_info:line.specialization}) set b.name = line.Hospital_names, b.country = line.country, b.city = tointeger(line.city) , b.reg_date=line.reg_date, c.first_name = line.first_name, c.Last_name = line.Last_name, e.specialization = line.activity_area;")
    database.run("load csv with headers from 'file:///datacsv/data"+i+".csv'as line FIELDTERMINATOR',' with line match (c:hospital {hospital_id: tointeger(line.hospital_id)}) match (p:person{email: (line.email)}) match (a:services{no_of_beds:tointeger(line.No_of_beds)}) match (s:specialization {specialization_info:line.specialization}) merge (a)-[:aconnectedtoc]->(c) merge (s)-[:sconnectedtoc]->(c) merge (p)-[:pconnectedtoc]->(c)")

    for i in range(0,31):
        num = database.run("MATCH (n:person) return n")
        time = num.consume()
        xn.append(int(time.result_available_after))
        num = database.run("MATCH (n:hospital) where n.country='Italy' return n")
        time = num.consume()
        xn1.append(int(time.result_available_after))
        num = database.run("MATCH (s:specialization)-[r:sconnectedtoc]->(c:hospital) where s.specialization = 'Surgery' return c.Hospital_name, c.hospital_id, c.country")
        time = num.consume()
        xn2.append(int(time.result_available_after))
        num = database.run("match (a:services)-[:aconnectedtoc]->(c:hospital)<-[:pconnectedtoc]-(p:person) where a.no_of_beds>1000 and a.no_of_beds<1200 return a.no_of_beds, c.country,p.first_name, p.Last_name, c.city")
        time = num.consume()
        xn3.append(int(time.result_available_after))
        
        n.append(xn)
        n1.append(xn1)
        n2.append(xn2)
        n3.append(xn3)


    print(xn,xn1,xn2,xn3)
    database.run("match (n) call {with n optional MATCH (n)-[r]-() DELETE n,r} in transactions of 2000 rows")
    database.run("drop constraint hospital_ids_db")
    database.run("drop constraint person_ids_db")
    database.run("drop constraint services_ids_db")
    database.run("drop constraint specialization_ids_db")