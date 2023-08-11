# Kafka and Spark Streaming Integration

## Overview

In this project, we provide a statistical analyses of the data using Apache Spark Structured Streaming. We created a Kafka server to produce data, a test Kafka Consumer to consume data and ingest data through Spark Structured Streaming. Then we applied Spark Streaming windowing and filtering to aggregate the data and extract count on hourly basis.

### Environment

 - Java 1.8.x
 - Python 3.6 or above
 - Zookeeper
 - Kafka
 - Scala 2.11.x
 - Spark 2.4.x


### How to Run?
#### Start Zookeeper and Kafka Server 
```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```
#### Run Kafka Producer server
`python kafka_server.py`

#### Run the kafka Consumer server 
`python kafka_consumer.py`

#### Submit Spark Streaming Job
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4 --master local[*] data_stream.py`



### kafka consumer console output
![Consumer console output
	    ](https://github.com/san089/SF-Crime-Statistics/blob/master/kafka-console-consumer-output.PNG)





### Streaming progress reporter
![Progress Reporter
	    ](https://github.com/san089/SF-Crime-Statistics/blob/master/spark-streaming-progress-report.PNG)


### Output
![output
	    ](https://github.com/san089/SF-Crime-Statistics/blob/master/output.png)

#### Reference: [https://spark.apache.org/docs/latest/sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)



