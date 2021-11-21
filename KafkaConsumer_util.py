from kafka import KafkaConsumer
import json

def readKafkaEvent():

    bootstrap_servers = ['10.2.52.146:9092', '10.2.52.162:9092', '10.2.52.148:9092']

    topicName = 'integration-ship-guest-core.guest'
    consumer = KafkaConsumer(topicName, bootstrap_servers=bootstrap_servers, auto_offset_reset = 'earliest',
                             enable_auto_commit = True, value_deserializer = lambda m: json.loads(m.decode("utf8")))

    for msg in consumer:

        print(msg.value)

if __name__ == '__main__':

    readKafkaEvent()
