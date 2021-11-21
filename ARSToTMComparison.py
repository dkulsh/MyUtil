import psycopg2
from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime

def readTabMgmtData(env):
    try:
        connection = connectTabMgmtDatabase(env)
        cursor = connection.cursor()
        tmQuery = "select external_id from reservations_reservation rr where addeddate > (CURRENT_DATE - INTERVAL '7 days')"
        # tmQuery = "select external_id from reservations_reservation rr where now() < timestamp and timestamp< (now() + INTERVAL '60 days')"
        print(tmQuery)
        cursor.execute(tmQuery)

        tmResponse = cursor.fetchall()
        tmReservations = []
        for row in tmResponse:
            tmReservations.append(row[0])

        return tmReservations

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def connectTabMgmtDatabase(env):

    if env == 'Prod':
        return psycopg2.connect(user="pgreadonlyuser",
                                      password="ENQvQPh4MOITtJvI",
                                      host="vv-prod-vxp-pgdb.cz4e2a1fysom.us-east-1.rds.amazonaws.com",
                                      port="5432",
                                      database="table_management")
    elif env == 'Cert':
        return psycopg2.connect(user="pgreadonlyuser",
                                password="h3FjEgkK7MAQS97Qt9ks",
                                host="certpgdb.gcpshore.virginvoyages.com",
                                port="5432",
                                database="table_management")

def readARSData(env):
    try:
        connection = connectARSDatabase(env)
        cursor = connection.cursor()
        arsQuery = "select distinct bookinglinkid from activitybooking a " \
                   "where a.addeddate > (CURRENT_DATE - INTERVAL '7 days') " \
                   "and a.activitygroupcode = 'RT' group by a.bookinglinkid "
                # "where now() < startdatetime and startdatetime < (now() + INTERVAL '60 days') " \
        print(arsQuery)
        cursor.execute(arsQuery)

        arsResponse = cursor.fetchall()
        arsReservations = []
        for row in arsResponse:
            arsReservations.append(row[0])

        return arsReservations

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def connectARSDatabase(env):

    if env == 'Prod':
        return psycopg2.connect(user="pgreadonlyuser",
                                      password="ENQvQPh4MOITtJvI",
                                      host="vv-prod-vxp-pgdb.cz4e2a1fysom.us-east-1.rds.amazonaws.com",
                                      port="5432",
                                      database="ars")
    elif env == 'Cert':
        return psycopg2.connect(user="pgreadonlyuser",
                                password="h3FjEgkK7MAQS97Qt9ks",
                                host="certpgdb.gcpshore.virginvoyages.com",
                                port="5432",
                                database="ars")

def readKafkaEvent(inARS_notInTM):
    # bootstrap_servers = ['localhost:9092']
    bootstrap_servers = ['10.15.12.38:9092', '10.15.13.172:9092', '10.15.2.58:9092']

    topicName = 'ars.Booking'
    consumer = KafkaConsumer(topicName, bootstrap_servers=bootstrap_servers, auto_offset_reset = 'earliest',
                             enable_auto_commit = True, value_deserializer = lambda m: str(m.decode('utf-8')))
                             # value_deserializer=lambda m: json.loads(m.decode('ascii')))

    producer = KafkaProducer(bootstrap_servers = bootstrap_servers, value_serializer = lambda v: json.dumps(v).encode('utf-8'))


    for msg in consumer:

        jsonMessage = json.loads(msg.value)

        if jsonMessage['pl']['bookingLinkId'] in inARS_notInTM and jsonMessage['source'] != 'DevOpsUtil':
            print("DateTime = %s, Message = %s" % (datetime.fromtimestamp(msg.timestamp/1000), msg.value))
            print("Posting this on Kafka")
            jsonMessage['source'] = 'DevOpsUtil'
            producer.send(topicName, jsonMessage)

if __name__ == '__main__':

    env = 'Prod' # Cert or 'Prod'

    tmReservations = readTabMgmtData(env = env)
    arsReservations = readARSData(env = env)

    print("TabMgmt reservations", len(tmReservations))
    print("ARS reservations", len(arsReservations))

    inARS_notInTM = list(set(arsReservations) - set(tmReservations))

    print(len(inARS_notInTM), " Reservations that are in ARS, but NOT in TM" )
    print(*inARS_notInTM, sep = "\n")

    # readKafkaEvent(inARS_notInTM)
