import time
import mysql.connector
import pandas

connect = mysql.connector.connect(user='root',password='mypass',port=3306)
mariadb_cursor =connect.cursor()


for j in ('1', '2', '3', '4'):
    xm = []
    xm1 = []
    xm2 = []
    xm3 = []
    mariadb_cursor.execute('create database projdb')
    mariadb_cursor.execute('use projdb')
    dbread = pandas.read_csv("C:/Users/vinay/Desktop/datacsv/data" + j + ".csv")
    dbread = dbread.itertuples()
    mariadb_cursor.execute("create table hospital(hospital_name varchar(100),hospital_id varchar(100),country varchar(100),city varchar(150),reg_date int,emergency_no int,primary key(hospital_id))")
    mariadb_cursor.execute("create table person (first_name varchar(150),Last_name varchar(150),email varchar(150),hospital_id varchar(100),primary key(email),foreign key(hospital_id) references hospital(hospital_id))")
    mariadb_cursor.execute("create table service(No_of_beds int,hospital_id varchar(100) , primary key(hospital_id),foreign key(hospital_id) references hospital(hospital_id) ) ")
    mariadb_cursor.execute("create table specialization (specialization varchar(150),hospital_id varchar(100),activity_area varchar(150),primary key(hospital_id),foreign key(hospital_id) references hospital(hospital_id))")
    for rows in dbread:
        insert_query = ("insert into hospital(hospital_name,hospital_id,country,city,reg_date,emergency_no) values('"+rows.Hospital_names+"','"+rows.hospital_id+"','"+rows.country+"','"+rows.city+"',"+str(rows.reg_date)+","+str(rows.emergency_no)+")")
        mariadb_cursor.execute(insert_query)
        insert_query2 =("insert into person (email,first_name,Last_name,hospital_id)values('"+rows.email+"','"+rows.first_name+"','"+rows.Last_name+"','"+rows.hospital_id+"')")
        mariadb_cursor.execute(insert_query2)
        insert_query3 =("insert into service (No_of_beds,hospital_id) values ("+str(rows.No_of_beds)+",'"+rows.hospital_id+"')")
        mariadb_cursor.execute(insert_query3)
        insert_query4 = ("insert into specialization (hospital_id, activity_area,  specialization) values ('"+rows.hospital_id+"','"+rows.activity_area+"','"+rows.specialization+"')")
        mariadb_cursor.execute(insert_query4)
    connect.commit()
    
    for i in range(0,31):
        start_time = time.time()
        mariadb_cursor.execute("select*from person")
        mariadb_cursor.fetchall()
        xm.append(int((time.time() - start_time) * 1000))

        start_time = time.time()
        mariadb_cursor.execute("SELECT * FROM hospital WHERE country = 'italy'")
        mariadb_cursor.fetchall()
        xm1.append(int((time.time() - start_time) * 1000))

        start_time = time.time()
        mariadb_cursor.execute("SELECT * FROM specialization WHERE specialization='gynecology'")
        mariadb_cursor.fetchall()
        xm2.append(int((time.time() - start_time) * 1000))

        start_time = time.time()
        mariadb_cursor.execute("SELECT * FROM service WHERE No_of_beds > 500 AND No_of_beds < 1200")
        mariadb_cursor.fetchall()
        xm3.append(int((time.time() - start_time) * 1000))

        
        
        
    mariadb_cursor.execute('drop database projdb')
        
    print(xm,xm1,xm2,xm3)