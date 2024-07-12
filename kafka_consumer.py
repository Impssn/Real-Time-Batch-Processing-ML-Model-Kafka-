from confluent_kafka import Consumer, KafkaError
import joblib
import json

# Load the saved model
model = joblib.load('california_model.pkl')

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

# Initialize the Kafka consumer
consumer = Consumer(conf)
consumer.subscribe(['house_prices'])

def consume_messages():
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        data = json.loads(msg.value().decode("utf-8"))
        features = data['features']
        prediction = model.predict([features])
        print(f'Received data: {features}, Prediction: {prediction[0]}')

if __name__ == '__main__':
    try:
        consume_messages()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
