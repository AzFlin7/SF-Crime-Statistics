from kafka import KafkaConsumer
import time

# Kafka consumer to test data moved to topic
class ConsumerServer(KafkaConsumer):
    
    def __init__(self, topic_name):
	
		# Configure a Kafka Consumer
        self.consumer = KafkaConsumer(
                        bootstrap_servers = "localhost:9092", 
                        request_timeout_ms = 1000, 
                        auto_offset_reset="earliest", 
                        max_poll_records=10
                        )   
        self.consumer.subscribe(topics = topic_name)
    
    def consume(self):
        try:
            while True:
                
				#Poll data for values
                for metadata,consumer_record in self.consumer.poll().items():
                    if consumer_record:
                        for record in consumer_record:
                            print(record.value)
                            time.sleep(0.2)
                    else:
                        print("No message received by consumer")
                        
                time.sleep(0.5)
        except:
            print("Closing consumer")
            self.consumer.close()
            
if __name__ == "__main__":
    consumer = ConsumerServer("police.service.calls")
    consumer.consume()