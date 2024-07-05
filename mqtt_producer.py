# cannot put sensor data directly to kafka as kafka is not equiped to handle to handle large no of topics 
# it many used for data collection through sensors/smart devices etc


import paho.mqtt.client as mqtt
from random import uniform
import time
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import json 


mqtt_broker = 'broker.emqx.io'
topic = 'TempSensor'
client_id = 'Producer'
mqtt_client = mqtt.Client()
mqtt_client.connect(mqtt_broker)


def run_publish(topic):
    while True:
        rand_num = uniform(0,150)
        msg = {"temperature": rand_num , "sensor_name": "TempSensor","timestamp": time.time()}
        msg = json.dumps(msg)
        result = mqtt_client.publish(topic, msg)
        status = result[0]
        if status ==0 :
            print(f"Send `{msg}` to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        time.sleep(3)

if __name__=='__main__':
    topic = 'TempSensor'
    run_publish(topic)






