
from cassandra.cluster import Cluster
import csv
import time
import pandas

# Connect to Cassandra
cluster = Cluster([''])
session = cluster.connect()

# Create keyspace and set the keyspace
session.execute("CREATE KEYSPACE IF NOT EXISTS testkeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
session.set_keyspace('testkeyspace')




# Load data from CSV files
for j in ('1', '2', '3', '4'):
    # Create tables
    session.execute("CREATE TABLE IF NOT EXISTS service (No_of_beds int, hospital_id text, PRIMARY KEY (hospital_id))")
    session.execute("CREATE TABLE IF NOT EXISTS hospital (hospital_id text, Hospital_names text, country text, city text, reg_date text, emergency_no text, PRIMARY KEY (hospital_id))")
    session.execute("CREATE TABLE IF NOT EXISTS person (first_name text, Last_name text, email text, hospital_id text, PRIMARY KEY (hospital_id, email))")
    session.execute("CREATE TABLE IF NOT EXISTS specialization (specialization text, hospital_id text, activity_area text, PRIMARY KEY (hospital_id, specialization))")
    dbread = pandas.read_csv("C:/Users/vinay/Desktop/datacsv/data"+j+".csv")
    dbread = dbread.to_dict(orient="records")
    for row in dbread:
        session.execute(f"INSERT INTO service (No_of_beds, hospital_id) VALUES ({row['No_of_beds']}, '{row['hospital_id']}')")
        session.execute(f"INSERT INTO hospital (hospital_id, Hospital_names, country, city, reg_date, emergency_no) VALUES ('{row['hospital_id']}', '{row['Hospital_names']}', '{row['country']}', '{row['city']}', '{row['reg_date']}', '{row['emergency_no']}')")
        session.execute(f"INSERT INTO person (first_name, Last_name, email, hospital_id) VALUES ('{row['first_name']}', '{row['Last_name']}', '{row['email']}', '{row['hospital_id']}')")
        session.execute(f"INSERT INTO specialization (specialization, hospital_id, activity_area) VALUES ('{row['specialization']}', '{row['hospital_id']}', '{row['activity_area']}')")

    # Perform queries and measure execution time
    xm = []
    xm1 = []
    xm2 = []
    xm3 = []

    for i in range(0, 31):
        start_time = time.time()
        rows = session.execute("SELECT * FROM person")
        xm.append(int((time.time() - start_time) * 1000))

        start_time = time.time()
        rows = session.execute("SELECT * FROM hospital WHERE country='italy' ALLOW FILTERING")
        xm1.append(int((time.time() - start_time) * 1000))

        start_time = time.time()
        rows = session.execute("SELECT * FROM specialization WHERE specialization='gynecology' ALLOW FILTERING")
        xm2.append(int((time.time() - start_time) * 1000))

        start_time = time.time()
        rows = session.execute("SELECT * FROM service WHERE No_of_beds > 500 AND No_of_beds < 1200 ALLOW FILTERING")
        xm3.append(int((time.time() - start_time) * 1000))

    print(xm, xm1, xm2, xm3)

    # Drop tables
    session.execute("DROP TABLE IF EXISTS service")
    session.execute("DROP TABLE IF EXISTS hospital")
    session.execute("DROP TABLE IF EXISTS person")
    session.execute("DROP TABLE IF EXISTS specialization")

# Close the Cassandra connection
cluster.shutdown()