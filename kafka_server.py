import producer_server
import pathlib

def run_kafka_server():
    
    """
    creates a Kafka producer
    """
    base_dir = pathlib.Path(__file__).parent
    input_file = base_dir.joinpath("police-department-calls-for-service.json")

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="police.calls",
        bootstrap_servers="localhost:9092",
        client_id="1"
    )

    return producer


def feed():
    
    """create a Kafka producer and send the police-department-calls-for-service.json file content
       to a Kafka topic
    """
    
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
