"""This module recieve message from MQTT broker and forward to AMQP broker"""

from paho.mqtt.client import MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR, MQTT_LOG_DEBUG
from paho.mqtt import client as mqtt
from module import alicloudAMQP
import configparser
import logging
import socket
import sys
import ssl
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
inftopic = config_obj["topic"]
infqueue = config_obj["queue"]
infmqtt = config_obj["mqtt"]
infamqp = config_obj["amqp"]
inflog = config_obj["log"]

groupId = infmqtt['groupid']
brokerUrl=infmqtt['endpoint']
exchange = str(infamqp['exchange'])
parent_topic = str(inftopic['parent'])

username = str(os.environ['FORWARDER_USER'])
password = str(os.environ['FORWARDER_PASS'])

topic = parent_topic+"/#"
recQueue = infqueue['devicerec']
ackQueue = infqueue['deviceack']
cardQueue = infqueue['devicecard']
hbQueue = infqueue['devicehb']
snapQueue = infqueue['devicesnap']
visitorQueue = infqueue['visitorsync']

client_id=groupId+'@@@'+socket.gethostname()+"-forwarder"
LOG_PATH = inflog['path']+"/inf-forwarder.log"

def on_log(client, userdata, level, buf):
    try:
        if level == MQTT_LOG_INFO:
            head = 'INFO'
        elif level == MQTT_LOG_NOTICE:
            head = 'NOTICE'
        elif level == MQTT_LOG_WARNING:
            head = 'WARN'
        elif level == MQTT_LOG_ERR:
            head = 'ERR'
        elif level == MQTT_LOG_DEBUG:
            head = 'DEBUG'
        else:
            head = level
        logger.debug('%s: %s' % (head, buf))

    except Exception as e:
        logger.error(str(e))

def on_connect(client, userdata, flags, rc):
    try : 
        logger.debug('Connected with result code ' + str(rc))
        client.subscribe(topic, 1)
        msg="Connected flags"+str(flags)+"result code "+str(rc)+"client1_id  "+str(client)
        logger.debug(msg)
    except Exception as e:
        logger.error(str(e))    

def on_message(client1, userdata, message):
    try :
        msg=str(message.payload.decode("utf-8"))
        logger.info(msg)

        tp = str(message.topic)
        if tp.endswith('/'):
            tp = tp.rstrip(tp[-1])

        routingKey = tp.replace("/",".")
        routingKey = routingKey.replace(parent_topic,exchange)

        logger.debug("routing Key : "+routingKey)
        elem = routingKey.split(".")

        last_element = str(elem[-1])
        if last_element == "Rec":
            queueName = recQueue
        elif last_element == "Ack":
            queueName = ackQueue
        elif last_element == "Card":
            queueName = cardQueue
        elif last_element == "Snap":
            queueName = snapQueue
        elif len(elem) == 2:
            queueName = routingKey.replace(".","-")
        else:
            queueName = hbQueue
        
        
        alicloudAMQP.amqpPublish(exchange,routingKey,msg,queueName)

    except Exception as e:
        logger.error(str(e))

def on_disconnect(client, userdata, rc):
    try : 
        if rc != 0:
            logger.debug('Unexpected disconnection %s' % rc)
    except Exception as e:
        logger.error(str(e))

if __name__ == "__main__":
    #Creating and Configuring Logger
    logger = logging.getLogger(client_id)
    fileHandler = logging.FileHandler(LOG_PATH)
    streamHandler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "function": "%(funcName)s", "message": "%(message)s"}')
    streamHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)
    logger.addHandler(fileHandler)
    logger.setLevel(logging.DEBUG)

    #reduce pika log level
    logging.getLogger("pika").setLevel(logging.WARNING)

    try :

        client = mqtt.Client(client_id, protocol=mqtt.MQTTv311, clean_session=False)
        client.on_log = on_log
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        client.username_pw_set(username, password)
        client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS, ciphers=None)
        client.connect(brokerUrl, 8883, 60)
        client.loop_forever()

    except Exception as e:
        if str(e) == "[Errno -2] Name or service not known":
            logger.error("Can't connect with "+brokerUrl+" , please check endpoint name in config file.")
        else:
            logger.error(str(e))