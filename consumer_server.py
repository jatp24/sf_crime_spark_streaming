from confluent_kafka import Consumer

BROKER_URL = "PLAINTEXT://localhost:9092"

def consumer_server(topic_name):



    broker_properties = {
        "bootstrap.servers": BROKER_URL,
        "group.id": 0,
        "auto.offset.reset": "earliest"
    }

    consumer = Consumer(broker_properties)
    consumer.subscribe([topic_name])


    while True:
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            print("no recieved messages")
        elif msg.error() is not None:
            print(f"error from consumer {msg.error()}")
        else:
            print(f"consumed message {msg.value()}")
                
if __name__ == "__main__":
    consumer_server('udacity.cops')

