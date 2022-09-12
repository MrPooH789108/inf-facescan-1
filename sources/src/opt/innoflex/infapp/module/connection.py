import configparser
import pika
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
infamqp = config_obj["amqp"]

username=str(os.environ['AMQP_USER'])
password=str(os.environ['AMQP_PASS'])

amqp_host = infamqp['endpoint']
amqp_port = infamqp['port']
virtualHost = infamqp['virtualHost']
amqp_instanceId = infamqp['instanceId']

def getConnectionParam():
    credentials = pika.PlainCredentials(
        username, password, erase_on_connect=True)
    return pika.ConnectionParameters(amqp_host, amqp_port, virtualHost, credentials)
