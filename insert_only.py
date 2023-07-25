import json
from confluent_kafka import Consumer, KafkaError

bootstrap_servers = 'localhost:9093'
topic = 'dbserver1.public.employees'

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
            kafka_message = json.loads(value.decode('utf-8'))

            if 'payload' in kafka_message and kafka_message['payload'] is not None:
                payload = kafka_message['payload']

                #! For INSERT OPERATION
                if 'op' in payload and payload['op'] == 'c':
                    if 'after' in payload and isinstance(payload['after'], dict):
                        emp_id = payload['after'].get('emp_id')
                        emp_name = payload['after'].get('emp_name')

                        print('\n\nInsert occurred!')
                        print(f'Employee ID: {emp_id}')
                        print(f'Employee Name: {emp_name}')
                        print('---------------------------------------------')

    except Exception as e:
        print('Error processing Kafka message:', e)


consumer = Consumer(consumer_conf)

consumer.subscribe([topic])

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
