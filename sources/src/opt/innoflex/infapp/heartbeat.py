"""This module recieve heartbeat message from devices"""

import sys
import ast
import pika
import logging
import threading
import configparser
from module import connection

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
infqueue = config_obj["queue"]
inflog = config_obj["log"]
infetc = config_obj["etc"]
infamqp = config_obj["amqp"]
amqp_host = infamqp['endpoint']
exchange = infamqp['exchange']
routing_key = exchange+".face.heartbeat"

queueName = infqueue['devicehb']

LOG_PATH = inflog['path']
THREADS = int(infetc['threadnum'])

logger = logging.getLogger('heartbeat-log')
logger.setLevel(logging.DEBUG)

fileFormat = logging.Formatter('{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')
fileHandler = logging.FileHandler(LOG_PATH+"/"+"heartbeat.log")        
fileHandler.setFormatter(fileFormat)
fileHandler.setLevel(logging.INFO)
logger.addHandler(fileHandler)

streamFormat = logging.Formatter('%(asctime)s %(name)s [%(levelname)s] %(message)s')
streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setFormatter(streamFormat)
streamHandler.setLevel(logging.DEBUG)
logger.addHandler(streamHandler)

#reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

        
class HeartbeatHandler(threading.Thread):
    def __init__(self):
        try : 
            threading.Thread.__init__(self)
            connect = pika.BlockingConnection(connection.getConnectionParam())
            self.channel = connect.channel()
            self.channel.queue_declare(queueName, durable=True, auto_delete=False)
            self.channel.basic_qos(prefetch_count=THREADS*10)
            threading.Thread(target=self.channel.basic_consume(
                queueName, on_message_callback=self.on_message))
                
        except Exception as e:
            if str(e) == "[Errno -2] Name or service not known":
                logger.debug("Can't connect with "+amqp_host+" , please check endpoint name in config file.")
            else:
                logger.error(str(e))

    def on_message(self, channel, method_frame, header_frame, body):
        try:
            body = str(body.decode())
            body = body.replace('\\r\\n', '')
            body = body.replace('\\', '')
            body = body[1:]
            body = body[:-1]
            
            message = ast.literal_eval(body)
            operation = message["operator"]
            facedevice = message["info"]["facesluiceId"]
            deviceCode = facedevice.split("@@@")[1]

            if operation == "HeartBeat":
                logger.info(body)

            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except Exception as e:
            logger.error("Error on "+str(e)+", or Invalid message format -- drop message")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def run(self):
        try:
            logger.debug('starting thread to consume from AMQP...')
            self.channel.start_consuming()

        except Exception as e:
            logger.error(str(e))

def main():
    for i in range(THREADS):
        logger.debug('launch thread '+str(i))
        td = HeartbeatHandler()
        td.start()

if __name__ == "__main__":
    main()