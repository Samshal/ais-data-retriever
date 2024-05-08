import asyncio
import websockets
import json
from datetime import datetime, timezone
import sqlite3

async def connect_ais_stream():

    try:    
        con = sqlite3.connect("aisdata.db")
        cur = con.cursor()

        async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
            subscribe_message = {"APIKey": "", "BoundingBoxes": [[[40.07679081, -23.72985398], [-17.77832738, 14.74979619]]]}

            subscribe_message_json = json.dumps(subscribe_message)
            await websocket.send(subscribe_message_json)

            async for message_json in websocket:
                try:
                    message = json.loads(message_json)
                    print(message)
                    message_type = message["MessageType"]
                    message_packet = message["Message"][message_type]
                    message_metadata = message["MetaData"]

                    query = f"INSERT INTO Messages (MessagePacket, MessageType, Metadata, Timestamp) VALUES ('{json.dumps(message_packet)}', '{message_type}', '{json.dumps(message_metadata)}', '{datetime.now(timezone.utc)}')";
                    cur.execute(query)
                    con.commit()

                    if message_type == "PositionReport":
                        # the message parameter contains a key of the message type which contains the message itself
                        ais_message = message['Message']['PositionReport']
                        print(f"[{datetime.now(timezone.utc)}] ShipId: {ais_message['UserID']} Latitude: {ais_message['Latitude']} Longitude: {ais_message['Longitude']}")
                except:
                    print("Error occurred here")
                    pass
    except:
        print("Error occurred")

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())
    

    # res = cur.execute("SELECT * FROM Messages")

    # print(res.fetchall())




