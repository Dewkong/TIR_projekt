import json
import sys
import time

import mosquitto

mqttc_server = sys.argv[1]
id = int(sys.argv[2])
time_to_water = int(sys.argv[3])
sector_id = None

mqttc = mosquitto.Mosquitto()

def on_connect(mqttc, obj, rc):
    print("rc: "+ str(rc))

def on_message(mqttc, obj, msg):
    global sector_id
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
    if msg.topic == "agh/iot/project9/config":
        try:
            msg_dict = json.loads(msg.payload)
            for sector in msg_dict["sectors"]:
                for sprinkler in sector["sprinklers"]:
                    if sprinkler == id:
                        sector_id = int(sector["id"])
        except Exception as e:
            print("json with incorrect format, " + str(e))
    elif msg.topic == "agh/iot/project9/active_sector":
        if int(msg.payload) == sector_id:
            mqttc.publish("agh/iot/project9/sprinkler/" + str(id) + "/state", "1", 2, False)
            before_watering = {
                "rain": None,
                "sprinkler_id": id,
                "is_watering": True
            }
            mqttc.publish("agh/iot/project9/simulation/area/" + str(sector_id) + "/rain", json.dumps(before_watering), 2, False)
            time.sleep(time_to_water)
            after_watering = {
                "rain": None,
                "sprinkler_id": id,
                "is_watering": False
            }
            mqttc.publish("agh/iot/project9/simulation/area/" + str(sector_id) + "/rain", json.dumps(after_watering), 2, False)
            mqttc.publish("agh/iot/project9/sprinkler/" + str(id) + "/state", "0", 2, False)


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
