import csv
import json
import requests
import psycopg2

def connectDB():
    try:
        connection = psycopg2.connect(user="pgreadonlyuser",
                                      password="ENQvQPh4MOITtJvI",
                                      host="vv-prod-vxp-pgdb.cz4e2a1fysom.us-east-1.rds.amazonaws.com",
                                      port="5432",
                                      database="batch_job")
        cursor = connection.cursor()
        readFile(cursor)

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

def readFile(cursor):
    file1 = open('/Users/deepkulshreshtha/Downloads/resNumbers.txt', 'r')
    lines = file1.readlines()

    for line in lines:
        # print("{}".format(line.strip()))
        queryDB(cursor, line.strip())
    # queryDB(cursor, '92363')

def queryDB(cursor, resNumber):

        sentToRRDQuery = "select * from entityhash e  where entitycode = 'W' and hashcode ilike ('vvwo" + resNumber + "r%')"
        # print(sentToRRDQuery)
        cursor.execute(sentToRRDQuery)
        sentResponse = cursor.fetchall()

        receviedFromRRDQuery = "select * from entityhash e  where entitycode = 'W' and hashcode ilike ('vvwof" + resNumber + "______________.csv')"
        # print(receviedFromRRDQuery)
        cursor.execute(receviedFromRRDQuery)
        receivedResponse = cursor.fetchall()

        if len(receivedResponse) == 0 and len(sentResponse) != 0:
            print("File was sent but NOT received for :: " + resNumber)
        elif len(receivedResponse) == 0 and len(sentResponse) == 0:
            print("File was NEVER sent for :: " + resNumber)

        # sent = received = 0
        # sentData = receivedData = []

        # print("Print each row and it's columns values")
        for row in sentResponse:
            print("Reservation number :: " + resNumber)
            print("Filename = ", row[4],)
            print("Status = ", row[5])
            print("File sent to RRD  = ", row[13], "\n")

            # if row[5] == 'Success_To_Fulfillment':
            #     sent += 1
            #     sentData.append({row[4], row[5], row[13]})
            # elif row[5] == 'Success_Remote_Delete':
            #     received += 1
            #     sentData.append({row[4], row[5], row[13]})
            # else:
            #     print("Status is ::" + row[5])

        # if sent > 0 or received > 0:
        #     print("Sent " + str(sent) + " rows to RRD for res number :: " + str(resNumber))
        #     print(*sentData, sep = "\n")
        #     print("Received " + str(received) + " rows from RRD for res number :: " + str(resNumber))
        #     print(*receivedData, sep = "\n")


        # sent = received = 0
        # sentData = receivedData = []

        # print("Print each row and it's columns values")
        for row in receivedResponse:
            print("Reservation number :: " + resNumber)
            print("Filename = ", row[4],)
            print("Status = ", row[5])
            print("File received from RRD  = ", row[13], "\n")

            lookupTrackables(row[13].split(',')[6])
            lookupTrackables(row[13].split(',')[10])

def lookupTrackables(resGuestID):

    # api-endpoint
    HEADER = {
        'Authorization' : "bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJjb21wYW55aWQiOiJiMzVhMzI1YS0wMzZhLTRhZDQtODgzNi02NDY5MjNlNjAxZGUiLCJzY29wZSI6WyJyZWFkIiwidHJ1c3QiLCJ3cml0ZSJdLCJleHAiOjE2Mjg2MjIxNDgsInRva2VuVHlwZSI6ImNsaWVudFRva2VuIiwiYXV0aG9yaXRpZXMiOlsiUk9MRV9UUlVTVEVEX0NMSUVOVCJdLCJqdGkiOiJiMDYwMzQ4Mi1hYjBkLTRiOWMtOGE5Zi1mODU4YWQyNmY2ODEiLCJjbGllbnRfaWQiOiJiM2MyYjA3Yy04NjkwLTExZTktYmQ4ZS0wYTFhNDI2MWU5NjIifQ.krIljM-zKn3XIawpUouXZxptrxRcQF9jxhmFNqWZCPzdlwfIVxgMwzhXglbwNrlwCX5qPmfz6FSn2ofFD4mp_P_ceXBLOJ7w9bh9PkHqMPrw5N72f23teQSdf2ohPdY4sYy82Nv4KH04xYzKDQ1Ja0S-I-7x-pHx39iVfe-vzulwBklP5LVALU2HZS8R03-dk5nPhQ26e3jwEkqeuwv1zfb8J6ymHph8kodulWE4scfOd47VpzinYxp7ouub9CDtOGP8N-rf-o36fipAO301d33YuXQBFQpo51kuOkOfMSbAJ2iOnnWeTQ0dm95TbkOu3HamX2oXmGCdhW1l5XoubQ",
        'Content-Type' : 'application/json'
    }
    HYDRATION_URL = "https://prod.virginvoyages.com/svc/hydration-service/trackablehosts/search"
    DATA = {
            "hostIds": [ resGuestID ],
            "hostIdType": "RG"
        }

    r = requests.post(url=HYDRATION_URL, data=json.dumps(DATA), headers=HEADER)
    data = r.json()

    if len(data['trackableHosts']):
        print("Trackable was assigned to reservationGuest : " + resGuestID)
    else:
        print("Trackable was NOT assigned to reservationGuest : " + resGuestID)

if __name__ == '__main__':
    connectDB()



