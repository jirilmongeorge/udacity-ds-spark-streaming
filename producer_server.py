from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):
    
    """ 
    class that inherits KafkaProducer
    """

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        """method that sends the input_file contents to Kafka topic"""
        with open(self.input_file) as f:
            call_data = json.load(f)
            for line in call_data:
                message = self.dict_to_binary(line)
     
                self.send(self.topic, value=message)
                time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        """method that converts python dictionary to JSON"""
        return json.dumps(json_dict).encode("utf-8")