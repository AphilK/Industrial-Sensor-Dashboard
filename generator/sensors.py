import time
import json
import random
import logging
import datetime
import threading
from paho import mqtt
import paho.mqtt.client as paho

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
TOPIC_BASE = "factory/assets"
TOPIC_ITEM = "factory/items"
MAX_CONCURRENT_ITEMS = 3
ITEM_LAUNCH_INTERVAL = (10, 15)
CLIENT_ID = f"user_{random.randint(0, 100)}"

connected = False

CYCLE_TIME_RANGES = {
    "robot1": (2.0, 7.0),
    "robot2": (2.0, 7.0),
    "robot3": (2.0, 7.0),
    "robot4": (2.0, 7.0),
    "robot5": (2.0, 7.0),
    "robot6": (2.0, 7.0),
    "robot7": (2.0, 7.0),
    "robot8": (2.0, 7.0)
}

LINE_STATIONS = ["robot1", "robot2", "robot3", "robot4", "robot5", "robot6", "robot7", "robot8"]

station_locks = {station: threading.Lock() for station in LINE_STATIONS}

def on_connect(client, userdata, flags, reason_code, properties=None):
    global connected
    if reason_code == 0:
        print(f"Logged to the MQTT Broker: {MQTT_BROKER} (ClientID: {CLIENT_ID})")
        connected = True
    else:
        print(f"Fail to connect, reason code: {reason_code}")
        connected = False

def on_disconnect(client, userdata, reason_code, properties=None):
    global connected
    print("Disconnected")
    connected = False

def publish_event(client, station_id, status, cycle_time=None):
    if status == "working":
        power_kw = random.uniform(0.7, 1.1)
        temperature = round(random.uniform(40, 60), 1)
        vibration = round(random.uniform(0.5, 4.5), 1)
        pressure = round(random.uniform(2, 10), 1)
        cycle_time_for_this_item = (round(cycle_time, 1) if cycle_time else "")
    else:
        #idle
        power_kw = 0.1
        temperature = round(random.uniform(25, 30), 1)
        vibration = round(random.uniform(0, 0.5), 1)
        pressure = round(random.uniform(0, 1), 1)
        cycle_time_for_this_item = 10

    data = {
        'asset_id': station_id,
        'status': status,
        'power_consumption': power_kw,
        'temperature': temperature,
        'vibration': vibration,
        'pressure': pressure,
        'cycle_time': cycle_time_for_this_item,
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

    topic = f"{TOPIC_BASE}/{station_id}"
    payload = json.dumps(data)
    result = client.publish(topic, payload)
    pub_status = result[0]
    if pub_status == 0:
        print(f"[{station_id}] -> {status}" + (f"(cycle: {cycle_time:.1f}s)" if cycle_time else ""))
    else:
        print(f"[{station_id}] ERROR when publishing")

def publish_item(client, count):
    data = {
        'count': count,
        'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

    payload = json.dumps(data)
    result = client.publish(TOPIC_ITEM, payload)
    status = result[0]
    if status == 0:
        print(f"Published in '{TOPIC_ITEM}': count = {count}")
    else:
        print(f"Failed to publish in '{TOPIC_ITEM}'")

def process_item(client, item_number):
    print(f"\n>>> Item #{item_number} entering in line")

    for station_id in LINE_STATIONS:
        with station_locks[station_id]:
            while not connected:
                time.sleep(1)

            min_time, max_time = CYCLE_TIME_RANGES[station_id]
            cycle_time = random.uniform(min_time, max_time)

            publish_event(client, station_id, "working", cycle_time)

            sleep_start = time.time()
            while(time.time() - sleep_start) < cycle_time:
                if not connected:
                    while not connected:
                        time.sleep(1)
                time.sleep(0.5)

            publish_event(client, station_id, "idle")

    print(f"<<< Item #{item_number} leaved the line")

def run_simulation():
    client = paho.Client(client_id=CLIENT_ID, protocol=paho.MQTTv5)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        print(f"Error when connectiong to the broker {MQTT_BROKER}: {e}")
        return
    
    client.loop_start()

    print("Waiting connection to the MQTT broker...")
    timeout_counter = 0
    while not connected and timeout_counter < 30:
        time.sleep(1)
        timeout_counter += 1

    if not connected:
        print("Failed to connect after 30 seconds. Shutting down...")
        client.loop_stop()
        return
    
    print("Starting simulation (Press Ctrl + C to stop)")
    time.sleep(2)

    item_counter = 1
    active_threads = []

    print("Factory opening.. Publishing 'idle' to all stations")
    for station in LINE_STATIONS:
        publish_event(client, station, "idle")

    print("Idle states published.")
    time.sleep(1)
    
    try:
        print(f"\n=== MAX {MAX_CONCURRENT_ITEMS} ITEMS CONCURRENT ===")
        print(f"=== Interval between launches: {ITEM_LAUNCH_INTERVAL[0]}-{ITEM_LAUNCH_INTERVAL[1]}s ===\n")
        time.sleep(2)

        while True:
            while not connected:
                print("Pausing... Waiting for MQTT reconection")
                time.sleep(1)

            active_threads = [t for t in active_threads if t.is_alive()]

            if len(active_threads) < MAX_CONCURRENT_ITEMS:
                current_item = item_counter
                item_counter += 1

                publish_item(client, current_item)

                item_thread = threading.Thread(
                    target=process_item,
                    args=(client, current_item),
                    daemon=True,
                    name=f"Item-{current_item}"
                )

                item_thread.start()
                active_threads.append(item_thread)

                print(f"Launched item #{current_item} (Active: {len(active_threads)})")

                launch_interval = random.uniform(ITEM_LAUNCH_INTERVAL[0], ITEM_LAUNCH_INTERVAL[1])
                print(f"Next launch in {launch_interval:.1f}s\n")
                time.sleep(launch_interval)

            else:
                time.sleep(1)

    except KeyboardInterrupt:
        print("\n\nSImulation interrupted by user")
        print(f"Waiting {len(active_threads)} item(s) finish...")
        for thread in active_threads:
            thread.join(timeout=5)
    finally:
        print("Closing factory...")
        client.loop_stop()
        client.disconnect()
        print("Finished.")


run_simulation()