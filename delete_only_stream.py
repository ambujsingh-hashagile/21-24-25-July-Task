import json
from confluent_kafka import Consumer, KafkaError

# Kafka broker settings
bootstrap_servers = 'localhost:9093'
topic = 'dbserver1.public.insert_only_stream'

# Create consumer configuration
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'Combined Lag',
    'auto.offset.reset': 'earliest'
}


def process_message(message):
    try:
        print(f"Received Kafka message: {message}")
        if message is None:
            print("Message is None. Skipping processing.")
            return

        value = message.value()
        if value is not None:
            # Deserialize JSON data into a dictionary
            kafka_message = json.loads(value.decode('utf-8'))

            # Check if the 'payload' key exists and is not None
            if 'payload' in kafka_message and kafka_message['payload'] is not None:
                payload = kafka_message['payload']

                if 'op' in payload and payload['op'] == 'd':
                    # Check if the 'before' key exists and is a dictionary
                    if 'before' in payload and isinstance(payload['before'], dict):
                        given_id = payload['before'].get('id')
                        given_value = payload['before'].get('value1')

                        print('Delete occurred!')
                        print(f'Deleted Employee ID: {given_id}')
                        print(f'Deleted Employee Name: {given_value} \n')

    except Exception as e:
        print('Error processing Kafka message:', e)


# Create the Kafka consumer
consumer = Consumer(consumer_conf)

# Subscribe to the topic
consumer.subscribe([topic])

# Poll for new messages and process them
try:
    while True:
        message = consumer.poll(1.0)

        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached.')
            else:
                print('Error while consuming message: {}'.format(message.error()))
        else:
            process_message(message)

except KeyboardInterrupt:
    print('Interrupted by user.')
finally:
    consumer.close()
