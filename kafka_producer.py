from confluent_kafka import Producer
import json

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

# Initialize the Kafka producer
producer = Producer(conf)

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def send_message(data):
    producer.produce('house_prices', value=json.dumps(data), callback=delivery_report)
    producer.poll(0)

if __name__ == '__main__':
    # List of multiple feature sets to be sent
    feature_sets = [
        {'features': [8.3252, 41, 6.984127, 1.02381, 322, 2.555556, 37.88, -122.23]},
        {'features': [8.3014, 21, 6.238137, 0.97687, 240, 2.109844, 37.86, -122.22]},
        {'features': [7.2574, 52, 5.6431, 1.07341, 497, 1.486602, 37.85, -122.23]},
        {'features': [7.2574, 52, 5.6431, 1.07341, 497, 1.486602, 37.85, -122.23]},
        {'features': [7.5, 23, 5.6, 1.0, 497, 1.48, 37.15, -112.23]},
        {'features': [7.5, 23, 56, 1.0, 470, 1.48, 37.15, -112.23]},
        # Add more feature sets as needed
    ]

    for feature_set in feature_sets:
        send_message(feature_set)

    producer.flush()
