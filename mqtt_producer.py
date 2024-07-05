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


# def delivery_report(err, msg):
#     """
#     Reports the failure or success of a message delivery.

#     Args:
#         err (KafkaError): The error that occurred on None on success.

#         msg (Message): The message that was produced or failed.

#     Note:
#         In the delivery report callback the Message.key() and Message.value()
#         will be the binary format as encoded by any configured Serializers and
#         not the same object that was passed to produce().
#         If you wish to pass the original object(s) for key and value to delivery
#         report callback we recommend a bound callback or lambda where you pass
#         the objects along.

#     """
#     if err is not None:
#         print("Delivery failed for User record {}: {}".format(msg.key(), err))
#         return
#     print('User record {} successfully produced to {} [{}] at offset {}'.format(
#         msg.key(), msg.topic(), msg.partition(), msg.offset()))

# # Define Kafka configuration
# # call these configurations come from api key text download when creating cluster
# kafka_config = {
#     'bootstrap.servers': 'pkc-41mxj.uksouth.azure.confluent.cloud:9092',
#     'sasl.mechanisms': 'PLAIN',
#     'security.protocol': 'SASL_SSL',
#     'sasl.username': 'JCSQVYZDB7OJCWRH',
#     'sasl.password': 'td1J2SE22HzYaTfqXHtqhEmQcLc3iiV24UAle9HY7PxMjGLKK/MKxHcHG7HtwYtB'
# }

# # Create a Schema Registry client
# # a single schema registry can handle multiple versions of schema as well as multiple schema for mutliple topics 
# # create a api key text doc , which will habve info schema registry client 
# schema_registry_client = SchemaRegistryClient({
#   'url': 'https://psrc-0j199.westeurope.azure.confluent.cloud',
#   'basic.auth.user.info': '{}:{}'.format('BJD4SQLACBLIOC4J', 'bxWfkNhBeqD7ZX7tr1PuND3+LiaaMPEyh0O9PyCGzUD2l7GElvCGr8PyYzhmzekd')
# })

# # Fetch the latest Avro schema for the value
# # while creating schema regsitry , subject name was asked 
# subject_name = 'temperature'
# schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
# print(schema_str)

# # Create Avro Serializer for the value
# # key_serializer = AvroSerializer(schema_registry_client=schema_registry_client, schema_str='{"type": "string"}')
# key_serializer = StringSerializer('utf_8')
# avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# # Define the SerializingProducer
# # info on what type of authentication to be used :https://developer.confluent.io/courses/security/authentication-ssl-and-sasl-ssl/#:~:text=SASL%2DSSL%20(Simple%20Authentication%20and,256%2F512%2C%20or%20OAUTHBEARER.
# # There are 4 types of authentications , we are using plain here 
# producer = SerializingProducer({
#     'bootstrap.servers': kafka_config['bootstrap.servers'],
#     'security.protocol': kafka_config['security.protocol'],
#     'sasl.mechanisms': kafka_config['sasl.mechanisms'],
#     'sasl.username': kafka_config['sasl.username'],
#     'sasl.password': kafka_config['sasl.password'],
#     'key.serializer': key_serializer,  # Key will be serialized as a string
#     'value.serializer': avro_serializer  # Value will be serialized as Avro
# })

def run_publish(topic):
    msg = 1
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
        msg_count += 1
        if msg_count > 5:
            break
        time.sleep(3)

if __name__=='__main__':
    run_publish()






