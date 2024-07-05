import paho.mqtt.client as mqtt
from random import uniform
import time
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import json 
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd

topic = 'TempSensor'
client_id = 'Consumer'
mqtt_client = mqtt.Client()

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'JP5YA56EVPSK3P37',
    'sasl.password': 'L/U5YWCKXxVKMEKECWtQMvWD+NnawPjqVaay5QXvl5vOL+GUS4vt4OI4RraCSXSe'
}

# Create a Schema Registry client
# a single schema registry can handle multiple versions of schema as well as multiple schema for mutliple topics 
# create a api key text doc , which will habve info schema registry client 
schema_registry_client = SchemaRegistryClient({
  'url': 'https://psrc-0j199.westeurope.azure.confluent.cloud',
  'basic.auth.user.info': '{}:{}'.format('BJD4SQLACBLIOC4J', 'bxWfkNhBeqD7ZX7tr1PuND3+LiaaMPEyh0O9PyCGzUD2l7GElvCGr8PyYzhmzekd')
})

# Fetch the latest Avro schema for the value
# while creating schema regsitry , subject name was asked 
subject_name = 'temperature'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# Create Avro Serializer for the value
# key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
# info on what type of authentication to be used :https://developer.confluent.io/courses/security/authentication-ssl-and-sasl-ssl/#:~:text=SASL%2DSSL%20(Simple%20Authentication%20and,256%2F512%2C%20or%20OAUTHBEARER.
# There are 4 types of authentications , we are using plain here 
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer  # Value will be serialized as Avro
})


def on_connect(client,userdata,flags,rc):
    client.subscribe(topic)

def on_message(client, userdata, msg):
    print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    value_ = json.loads(msg.payload.decode())
    # Produce to Kafka
    # delivery report reports fail or success of delivery 
    producer.produce(topic=msg.topic , value=value_, on_delivery=delivery_report)
    # # producer publishes data to broker one after another , we can configure producer.flush in such a 
    # # way that producer flushes data only after 100 recors etc 
    producer.flush()


def main():
    mqtt_broker = 'broker.emqx.io'
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(mqtt_broker)
    mqtt_client.loop_forever()

if __name__ == '__main__':
    main()

