import pandas 
import redis
import csv
import time

m = []
m1 = []
m2 = []
m3 = []

# Connect to Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
#redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

for j in ('1', '2', '3', '4'):
    xm = []
    xm1 = []
    xm2 = []
    xm3 = []
    dbread = pandas.read_csv(f"C:/Users/vinay/Desktop/datacsv/data{j}.csv")
    dbread = dbread.to_dict(orient="records")

    for row in dbread:
        # Use Redis hash to store data
        redis_client.hset(f"service:{row['hospital_id']}", "No_of_beds", row['No_of_beds'])
        redis_client.hset(f"hospital:{row['hospital_id']}", "Hospital_names", row['Hospital_names'])
        redis_client.hset(f"hospital:{row['hospital_id']}", "country", row['country'])
        redis_client.hset(f"hospital:{row['hospital_id']}", "city", row['city'])
        redis_client.hset(f"hospital:{row['hospital_id']}", "reg_date", row['reg_date'])
        redis_client.hset(f"hospital:{row['hospital_id']}", "emergency_no", row['emergency_no'])
        redis_client.hset(f"person:{row['hospital_id']}", "first_name", row['first_name'])
        redis_client.hset(f"person:{row['hospital_id']}", "Last_name", row['Last_name'])
        redis_client.hset(f"person:{row['hospital_id']}", "email", row['email'])
        redis_client.hset(f"specialization:{row['hospital_id']}", "specialization", row['specialization'])
        redis_client.hset(f"specialization:{row['hospital_id']}", "activity_area", row['activity_area'])


    for i in range(0,31):
        start_time = time.time()
        # Query 1: Find all persons
        persons=[]
        for keys in redis_client.keys("person:*"):
            persons.append(redis_client.hgetall(keys))
        #print("Query 1 - All Persons:", persons)
        
        xm.append(int((time.time() - start_time) * 1000))

        
        start_time = time.time()
        # Query 2: Find hospitals in Italy
        italy_hospitals = []
        for key in redis_client.keys("hospital:*"):
            if redis_client.hget(key, "country") == b'Italy':
                italy_hospitals.append(redis_client.hgetall(key))
        #print("Query 2 - Hospitals in Italy:", italy_hospitals)
       
        xm1.append(int((time.time() - start_time) * 1000))


        start_time = time.time()
        # Query 3: Find hospitals with specialization in gynecology
        gynecology_hospitals = []
        for key in redis_client.keys("specialization:*"):
            if redis_client.hget(key, "specialization") == b'gynecology':
                hospital_id = key.decode().split(":")[1]
                hospital_info = redis_client.hgetall(f"hospital:{hospital_id}")
                gynecology_hospitals.append(hospital_info)
        #print("Query 3 - Gynecology Specialization:", gynecology_hospitals)
        
        xm2.append(int((time.time() - start_time) * 1000))


        start_time = time.time()
        # Query 4: Find hospitals with beds between 500 and 1200
        bed_range_hospitals = []
        for key in redis_client.keys("service:*"):
            beds = int(redis_client.hget(key, "No_of_beds"))
            if 500 < beds < 1200:
                hospital_id = key.decode().split(":")[1]
                hospital_info = redis_client.hgetall(f"hospital:{hospital_id}")
                bed_range_hospitals.append(hospital_info)
        #print("Query 4 - Hospitals with Beds between 500 and 1200:", bed_range_hospitals)
        
        xm3.append(int((time.time() - start_time) * 1000))
    
    # Clear Redis data at the end of each iteration (you may adjust this based on your use case)
    redis_client.flushdb()
    
    print(xm, xm1, xm2, xm3)