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

def setup_logger(name, log_file, level=logging.INFO):

    formatter = logging.Formatter('{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')

    fileHandler = logging.FileHandler(log_file)        
    fileHandler.setFormatter(formatter)

    streamHandler = logging.StreamHandler(sys.stdout)
    streamHandler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(streamHandler)
    logger.addHandler(fileHandler)

    #reduce pika log level
    logging.getLogger("pika").setLevel(logging.WARNING)

    return logger
        
class HeartbeatHandler(threading.Thread):
    def __init__(self):
        try : 
            threading.Thread.__init__(self)
            connect = pika.BlockingConnection(connection.getConnectionParam())
            self.channel = connect.channel()
            self.channel.queue_declare(queueName, durable=True, auto_delete=False)
            self.channel.queue_bind(exchange=exchange,queue=queueName,routing_key=routing_key)
            self.channel.basic_qos(prefetch_count=THREADS*10)
            threading.Thread(target=self.channel.basic_consume(
                queueName, on_message_callback=self.on_message))
                
        except Exception as e:
            if str(e) == "[Errno -2] Name or service not known":
                print("Can't connect with "+amqp_host+" , please check endpoint name in config file.")
            else:
                print(str(e))

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
                logger = setup_logger(str(deviceCode), LOG_PATH+"/"+str(deviceCode)+"_heartbeat.log")
                logger.info(body)

            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except Exception as e:
            logger = setup_logger('heartbeat-log', LOG_PATH+"/"+str(deviceCode)+"_heartbeat.log")
            logger.error(str(e))
            channel.basic_reject(delivery_tag=method_frame.delivery_tag)

    def run(self):
        try:
            print('starting thread to consume from AMQP...')
            self.channel.start_consuming()

        except Exception as e:
            print(str(e))

def main():
    for i in range(THREADS):
        print('launch thread '+str(i))
        td = HeartbeatHandler()
        td.start()

if __name__ == "__main__":

    main()
