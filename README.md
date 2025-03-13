
# 🏥 Database Performance Analysis for Hospital Data  

## 📌 Project Overview  
This project analyzes the performance of **Neo4j, MongoDB, MariaDB, Cassandra, and Redis** by executing queries on hospital-related data. The goal is to compare their runtime efficiency and determine the best-performing database for different query complexities.  

## 📊 Dataset  
- **Generated synthetic hospital data** using an online data generator  
- **Dataset size:** 25,000 to 100,000 records  
- **Tables:** Hospital, Person, Specialization, Service  
- **File format:** CSV  

## 🔍 Methodology  
### **1️⃣ Data Preparation & Setup**  
- **Docker containers** used for setting up the databases  
- **Data imported** into each database system  
- **Performed queries** on all databases to measure runtime  

### **2️⃣ Database Systems & Query Execution**  
#### 🟢 **Neo4j**  
- Graph database optimized for highly structured data  
- Used **Cypher Query Language (CQL)** for data retrieval  

#### 🔵 **MongoDB**  
- NoSQL document-oriented database with **BSON storage**  
- Data stored in **collections** instead of traditional tables  

#### 🟠 **MariaDB**  
- **Relational database** using SQL-based query execution  
- Used **foreign keys and table relations** for structured queries  

#### 🔴 **Cassandra**  
- **Highly scalable, distributed NoSQL database**  
- Optimized for **write-heavy workloads and fault tolerance**  

#### 🟣 **Redis**  
- **In-memory key-value store**, used for caching & real-time processing  
- Fastest retrieval but limited query capabilities  

## 📈 Performance Analysis  
- **Query 1:** Retrieve all persons' data  
- **Query 2:** Fetch hospital registrations in Italy  
- **Query 3:** Identify hospitals with gynecology specialization  
- **Query 4:** Find hospitals with bed count between 500 and 1200  

## 🚀 Results & Conclusion  
- **Neo4j & MongoDB** performed best for complex relationships  
- **MariaDB** was more efficient for structured queries  
- **Cassandra** had the **highest runtime**, especially on large datasets  
- **Redis** excelled in rapid data access but lacked relational querying  

## ⚙️ Technologies Used  
- **Python** for database interaction  
- **Docker** for setting up multiple databases  
- **Pandas & CSV** for data handling  

## 📌 How to Run the Project  
1. **Clone this repository**  
   ```bash
   git clone https://github.com/your-repo-name.git
   cd your-repo-name
   ```  
2. **Install dependencies**  
   ```bash
   pip install pymongo neo4j mysql-connector redis pandas
   ```  
3. **Run database queries**  
   ```bash
   python database_analysis.py
   ```  

## 📝 Future Improvements  
- **Optimize indexing & caching strategies** for better performance  
- **Expand dataset size** to analyze scalability under real-world loads  
- **Incorporate additional query types** for deeper analysis  

📌 **Contributors:**  
- **Arkala Chandhra Shekar Jyothi Vinay**  
- **Peram Divya**  
