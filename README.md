#The objective of this system is to capture live tweets from a simulated or real source, process them in real-time, store them in a relational database, 
and visualize the insights via a Flask-based dashboard. Additionally, the project evaluates and compares the performance of streaming vs batch processing, 
measuring metrics such as execution time and accuracy.

#Prerequisites
>	Python: Version 3.10 or above<br/>
>	Docker Desktop: For running containerized services
>	Java (JDK 8+): Required for Apache Spark
>	Kafka & Zookeeper: Set up within Docker
>	PostgreSQL: Set up within Docker
>	Apache Spark: Installed locally or containerized
>	VS Code / PyCharm (optional): For code editing

#Start Docker Containers
Ensure Docker is running, then start the services:
```
cd docker
docker-compose up --build
```

#Install Python Dependencies
```
pip install -r requirements.txt
```

#Create Topic 
```
docker exec -it docker-kafka-1 kafka-topics --create --topic tweets_streaming --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it docker-kafka-1 kafka-topics --create --topic tweets_batch --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

#Confirm
```
docker exec -it docker-kafka-1 kafka-topics --list --bootstrap-server localhost:9092
```

#Run the kafka consumer and producer
```
cd kafka
```
Terminal 1:
```
python producer.py
```
Terminal 2:
```
python consumer.py
```
#Check the Postresql for querying
Terminal 3:
```
docker exec -it docker-postgres-1 psql -U postgres -d twitter_data
SELECT * FROM tweets LIMIT 10;
```

#Run the spark batch processing and stream the data
Terminal 1:
```
spark-submit --jars "path/to/postgresql-42.7.3" "path/to/batch_job.py"
```
Terminal 2:
```
docker exec -it docker-postgres-1 psql -U postgres -d twitter_data
SELECT * FROM batch_tweets LIMIT 10;
```
