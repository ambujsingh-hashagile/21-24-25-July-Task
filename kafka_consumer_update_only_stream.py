import json
from confluent_kafka import Consumer, KafkaError

# Kafka broker settings
bootstrap_servers = 'localhost:9093'
topic = 'dbserver1.public.employees'

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

                # Check if the operation is an insert ('op' == 'c')
                if 'op' in payload and payload['op'] == 'u':
                    # Check if the 'after' key exists and is a dictionary
                    if 'after' in payload and isinstance(payload['after'], dict):
                        emp_id = payload['after'].get('emp_id')
                        emp_name = payload['after'].get('emp_name')

                        print('Update occurred!')
                        print(f'Employee ID: {emp_id}')
                        print(f'Employee Name: {emp_name}')

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
