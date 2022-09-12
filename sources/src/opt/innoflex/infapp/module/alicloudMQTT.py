from paho.mqtt.client import MQTT_LOG_INFO, MQTT_LOG_NOTICE, MQTT_LOG_WARNING, MQTT_LOG_ERR, MQTT_LOG_DEBUG
from paho.mqtt import client as mqtt
from hashlib import sha1
import configparser
import socket
import json
import time
import ssl
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
inftopic = config_obj["topic"]
infmqtt = config_obj["mqtt"]
infqueue = config_obj["queue"]

username = str(os.environ['CLIENT_USER'])
password = str(os.environ['CLIENT_PASS'])

instanceId = infmqtt['instanceId']
groupId = infmqtt['groupId']
brokerUrl = infmqtt['endpoint']

client_id=groupId+'@@@'+socket.gethostname()+"-client"

def on_log(client, userdata, level, buf):
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
    print('%s: %s' % (head, buf))


def on_connect(client, userdata, flags, rc):
    print('Connected with result code ' + str(rc))
    data = json.dumps(msg)
    print(data)
    rc = client.publish(topic, data, qos=1)
    print('rc: %s' % rc)
    time.sleep(1)
    client.loop_stop()  # Stop loop
    client.disconnect()  # disconnect


def on_message(client, userdata, msg):
    print(msg.topic + ' ' + str(msg.payload))


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print('Unexpected disconnection %s' % rc)


def mqttPublish(pub_msg, pub_topic):
    isSuccess = True
    try:
        global msg, topic
        msg = pub_msg
        topic = pub_topic
        
        client = mqtt.Client(
            client_id, protocol=mqtt.MQTTv311, clean_session=False)
        client.on_log = on_log
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        client.username_pw_set(username, password)
        client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS, ciphers=None)
        client.connect(brokerUrl, 8883, 60)
        client.loop_forever()
        return isSuccess

    except Exception as e:
        print(str(e))
        isSuccess = False
        return isSuccess
