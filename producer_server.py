from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        self.counter = 0

    #Read data from json file and feed to topic
    def generate_data(self):
        with open(self.input_file) as f:
            file_data = json.load(f)
            for row in file_data:
                message = self.dict_to_binary(row)
                # TODO send the correct data
                future_record = self.send(topic = self.topic,value=message)
                self.counter = self.counter + 1
                print(f"Record number : {self.counter}. Record : {future_record.get()}")
                time.sleep(0.2)
        

    # return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf8')
        