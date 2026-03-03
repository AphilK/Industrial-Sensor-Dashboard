import time
import json
import random
import psycopg2
import paho.mqtt.client as paho

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
TOPIC_BASE = "factory/assets"
TOPIC_ITEM = "factory/items"
CLIENT_ID = f"user_{random.randint(0, 100)}"

with psycopg2.connect(host='localhost', database='sensors_db', user='admin', password='12345') as con:
    cur = con.cursor()

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"Logged to the MQTT Broker: {MQTT_BROKER} (ClientID: {CLIENT_ID})")
        else:
            print(f"Fail to connect, reason code: {reason_code}")

        client.subscribe(f"{TOPIC_BASE}/#")

    def on_message(client, userdata, msg):
        try:
            data = json.loads(msg.payload.decode())
            for n in range(1, 9):
                if msg.topic == f"{TOPIC_BASE}/robot{n}":
                    sql = """INSERT INTO Sensors (Topic, Status, PowerConsumption, Temperature, Vibration, Pressure, CycleTime, TimeStamp)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
                    cur.execute(sql, (
                        msg.topic,
                        data["status"],
                        data["power_consumption"],
                        data["temperature"],
                        data["vibration"],
                        data["pressure"],
                        data["cycle_time"],
                        data["timestamp"]
                    ))
                    con.commit()
            print(msg.topic + " " + str(data))
        except Exception as e:
            print("Error processing message:", e)
            
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'Sensors'
        );
    """)
    exists = cur.fetchone()[0]

    if exists == False:
        print("Table Sensors does not exists...Creating....")
        cur.execute("""CREATE TABLE Sensors (
        Topic VARCHAR(255),
        Status VARCHAR(50),
        PowerConsumption REAL,
        Temperature REAL,
        Vibration REAL,
        Pressure REAL,
        CycleTime REAL,
        TimeStamp TIMESTAMP WITH TIME ZONE
    );""")
        
        print("Table Sensors created!")
        time.sleep(1)

    mqttc = paho.Client(client_id=CLIENT_ID, protocol=paho.MQTTv5)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message

    mqttc.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqttc.loop_forever()
    con.close()