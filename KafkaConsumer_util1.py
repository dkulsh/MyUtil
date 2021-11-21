
from kafka import KafkaConsumer
import json

def readKafkaEvent():

    bootstrap_servers = ['10.2.52.146:9092', '10.2.52.162:9092', '10.2.52.148:9092']

    topicName = 'integration-ship-core.ship'
    consumer = KafkaConsumer(topicName, bootstrap_servers=bootstrap_servers, auto_offset_reset = 'earliest',
                             enable_auto_commit = True)

    # , value_deserializer = lambda m: json.loads(m.decode("utf-8"))

    for msg in consumer:
        try:
            print(msg.value)
            print("Deserialized ", json.dumps(json.loads(msg.value.decode("utf-8"))), '\n')
        except Exception as e:
            print(f"Exception ({e}) occured for message {msg}!")


def decode():
    msg = b'{\n  "ci": "fc73560b-21b0-4947-8ea7-4f3b2dba997a",\n  "ts": "2021-09-07T11:26:50",\n  "n": "shiptime:updated",\n  "cn": "ship-service",\n  "tg": "shiptime updated",\n  "plt": "json",\n  "pl": {\n    "modifiedByUser": "49e14671-32d3-41e8-b579-d747b54ffd23",\n    "modificationTime": "2021-09-07T11:26:50",\n    "shipTimeId": "7ab60bb8-db1f-48fa-8033-be8fd52798ad",\n    "shipId": "da93577c-8b43-11e8-827d-0a1a4261e962",\n    "fromUtcDate": 1576610327000,\n    "fromDateOffset": +360,\n    "isDeleted": false,\n    "shipCode": "SC"\n  },\n  "mrc": 3,\n  "rc": 0,\n  "ri": 1800000,\n  "isSD": false,\n  "encrypted": false\n}'

    try:
        print()
        # print("Deserialized ", json.loads(msg.decode("utf-8")), '\n')
    except Exception as e:
        print(f"Exception ({e}) occured for message {msg}!")


if __name__ == '__main__':

    # readKafkaEvent()
    decode()
