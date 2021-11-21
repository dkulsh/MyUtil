import psycopg2

LATEST_SHORE = "latest-shore"
LATEST_SHIP = "latest-ship"
STAGE_SHORE = "stage-shore"
STAGE_SHIP = "stage-ship"
DXP_CORE = "dxpcore"
TM = "table_management"

def connectTabMgmtDatabase(env):

    if env == LATEST_SHORE:
        return psycopg2.connect(user="pgreadonlyuser",
                                password="QHY!uxX8pwM4sgkG29Jz",
                                host="10.117.137.30",
                                port="5432",
                                database=TM)

    if env == LATEST_SHIP:
        return psycopg2.connect(user="pgreadonlyuser",
                                password="yellow*99",
                                host="10.61.69.24",
                                port="4001",
                                database=TM)

    if env == STAGE_SHORE:
        return psycopg2.connect(user="pgreadonlyuser",
                                password="TanKDX8eS",
                                host="dcl-dxp-online-postgresql-stage.cdjmbnqi2xdo.us-east-1.rds.amazonaws.com",
                                port="5432",
                                database=TM)

    if env == STAGE_SHIP:
        return psycopg2.connect(user="pgreadonlyuser",
                                password="yellow*99",
                                host="10.92.78.156",
                                port="4001",
                                database=TM)

def readData(env, voyage_id):

    try:
        connection = connectTabMgmtDatabase(env)
        cursor = connection.cursor()

        tmQuery = f"select public_id from reservations_reservation rr where voyage_id = '{voyage_id}' and type = 'rotation'"
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

if __name__ == '__main__':

    voyage_id = '4bb56b17-a8c6-4914-a42d-3b63b70906ad'

    shoreReservations = readData(STAGE_SHORE, voyage_id)
    shipReservations = readData(STAGE_SHIP, voyage_id)

    print("Shore reservations", len(shoreReservations))
    print("Ship reservations", len(shipReservations))

    inShore_notInShip = list(set(shoreReservations) - set(shipReservations))
    print(len(inShore_notInShip), " In Shore, but not in Ship" )

    print("missing ids \t")
    print(inShore_notInShip)

    inShip_notInShore = list(set(shipReservations) - set(shoreReservations))
    print(len(inShip_notInShore), " In Ship, but not in Shore" )
    print("missing ids \t")
    print(inShip_notInShore)