from kafka import KafkaProducer

topic_name = 'items'
kafka_server = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=kafka_server)

producer.send(topic_name, b'Test Message!!!')
# b means we want to treat it as binary. This is a python specific syntax
producer.flush()