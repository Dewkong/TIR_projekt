import json
import sys
import time

import mosquitto

mqttc_server = sys.argv[1]
id = int(sys.argv[2])
sector_id = None
sensor_id = None
current_humidity = -1

mqttc = mosquitto.Mosquitto()

def on_connect(mqttc, obj, rc):
    print("rc: "+ str(rc))

def on_message(mqttc, obj, msg):
    global sector_id, sensor_id, current_humidity
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
    if msg.topic == "agh/iot/project9/config":
        try:
            msg_dict = json.loads(msg.payload)
            for sector in msg_dict["sectors"]:
                for sprinkler in sector["sprinklers"]:
                    if sprinkler["sprinkler_id"] == id:
                        sector_id = int(sector["id"])
                        sensor_id = int(sector["sensor_id"])
                        mqttc.subscribe("agh/iot/project9/sensor/" + str(sensor_id) + "/humidity", 0)
        except Exception as e:
            print("json with incorrect format, " + str(e))
    elif msg.topic == "agh/iot/project9/active_sector":
        if int(msg.payload) == sector_id:
            mqttc.publish("agh/iot/project9/sprinkler/" + str(id) + "/state", "1", 0, False)
            time_to_sleep = (11 - (current_humidity/10)) if current_humidity != -1 else 5
            time.sleep(time_to_sleep)
            mqttc.publish("agh/iot/project9/sprinkler/" + str(id) + "/state", "0", 0, False)
    elif msg.topic == "agh/iot/project9/sensor/" + str(sensor_id) + "/humidity":
        current_humidity = int(msg.payload)


def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))

def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_log(mqttc, obj, level, string):
    print(string)


mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_log = on_log
mqttc.connect(mqttc_server, 1883, 60)

mqttc.subscribe("agh/iot/project9/config", 0)
mqttc.subscribe("agh/iot/project9/active_sector", 0)

mqttc.loop_forever()
