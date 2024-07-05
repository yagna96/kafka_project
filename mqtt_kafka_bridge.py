import paho.mqtt.client as mqtt
from random import uniform
import time
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import json 


topic = 'TempSensor'
client_id = 'Consumer'
mqtt_client = mqtt.Client()

def on_connect(client,userdata,flags,rc):
    client.subscribe(topic)

def on_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")

def main():
    mqtt_broker = 'broker.emqx.io'
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(mqtt_broker)
    mqtt_client.loop_forever()

if __name__ == '__main__':
    main()

