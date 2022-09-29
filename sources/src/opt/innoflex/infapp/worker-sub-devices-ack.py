""" This module receive person ack from device """
""" worker-sub-devices-ack.py """

from asyncio.windows_events import NULL
from module import alicloudDatabase
from module import connection
from pymongo import MongoClient
from datetime import datetime
import configparser
import threading
import logging.handlers
import logging
import pika
import ast
import sys
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infcollection = config_obj["collection"]
infdatabase = config_obj["db"]
inftopic = config_obj["topic"]
infqueue = config_obj["queue"]
inflog = config_obj["log"]
infetc = config_obj["etc"]
infamqp = config_obj["amqp"]
infroute = config_obj["route"]
infoperation = config_obj["operation"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
workertb = infcollection['workers']
transectiontb = infcollection['transections']

create_worker_operation_name = infoperation['create_worker']

parent_topic = inftopic['parent']
queueName = infqueue['deviceAck']
exchange = infamqp['exchange']
route = str(infroute['deviceack'])
routing_key = exchange+"."+route

LOG_PATH = inflog['path']
THREADS = int(infetc['threadnum'])

logger = logging.getLogger('DeviceAck')
logger.setLevel(logging.DEBUG)

fileFormat = logging.Formatter('{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')
fileHandler = logging.FileHandler(LOG_PATH+"/inf-worker-sync.log")        
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

class ThreadedConsumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        connect = pika.BlockingConnection(connection.getConnectionParam())
        self.channel = connect.channel()
        self.channel.queue_declare(queueName, durable=True, auto_delete=False)
        self.channel.basic_qos(prefetch_count=THREADS*10)
        threading.Thread(target=self.channel.basic_consume(
            queueName, on_message_callback=self.on_message))

    def on_message(self, channel, method_frame, header_frame, body):
        try:
            logger.debug(method_frame.delivery_tag)
            body = str(body.decode())
            body = body.replace('\\r\\n', '')
            body = body.replace('\\', '')
            body = body[1:]
            body = body[:-1]
            logger.debug(body)

            message = ast.literal_eval(body)
            operation = message['operator']

            mydb = dbClient[dbName]
            mycol = mydb[transectiontb]

            if operation == "DelPerson-Ack":
                messageId = message['messageId']
                code = message['code']
                facedevice = str(message['info']['facesluiceId'])
                deviceCode = facedevice.split("@@@")[1]
                workerCode = message['info']['customId']

                if code != "200":
                    code_detail = message['info']['detail']
                else:
                    code_detail = 'No error message'

                logger.debug("operation : "+operation)
                logger.debug("messageId : "+messageId)
                logger.debug("code : "+code)
                logger.debug("facedevice : "+deviceCode)
                logger.debug("workerCode : "+workerCode)

                data = {
                    "messageId": messageId,
                    "deviceCode": deviceCode,
                    "workerCode": workerCode,
                    "code": code,
                    "code_detail": code_detail
                }

                myquery = {"messageId": messageId}
                transection = mycol.find(myquery)

                for t in transection:
                    sub_transection = t['transection']
                    oper = t['operation']
                    
                    for sub_t in sub_transection:
                        topic = sub_t['topic']
                        logger.debug("topic : "+topic)
                        if topic == parent_topic+"/face/"+deviceCode:
                            logger.debug("topic : "+topic)

                            # update on transection table
                            query = {"messageId": messageId, "transection": {
                                "$elemMatch": {"topic": topic, "ackcode": "wating ack"}}}
                            newvalues = {"$set": {
                                "transection.$.ackcode": code, "transection.$.ackdetail": code_detail}}
                            isSuccess = alicloudDatabase.updateOneToDB(
                                transectiontb, query, newvalues)
                            log = {
                                "data": data,
                                "tasks": {
                                    "database": {
                                        "collection": transectiontb,
                                        "operation": "update",
                                        "query": query,
                                        "newvalue": newvalues,
                                        "success": isSuccess
                                    }
                                }
                            }

                            logs = str(log)
                            logger.info(logs)

                            if code == "200" or code == "464":
                                if oper == create_worker_operation_name:
                                    # INACTIVE Worker
                                    # update on workerlist table
                                    dtime = datetime.now()
                                    last_update = dtime.strftime(
                                        "%Y-%m-%d %H:%M:%S")
                                    query = {"registration.last_messageId": messageId, "devices": {
                                        "$elemMatch": {"deviceCode": deviceCode}}}
                                    newvalues = {"$set": {"registration.last_update": last_update,
                                                          "devices.$.regester": "unregistered", "devices.$.last_update": last_update}}
                                    isSuccess = alicloudDatabase.updateOneToDB(
                                        workertb, query, newvalues)
                                    log = {
                                        "data": data,
                                        "tasks": {
                                            "database": {
                                                "collection": workertb,
                                                "operation": "update",
                                                "query": query,
                                                "newvalue": newvalues,
                                                "success": isSuccess
                                            }
                                        }
                                    }

                                    logs = str(log)
                                    logger.info(logs)

                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            elif operation == "EditPerson-Ack":
                messageId = message['messageId']
                code = str(message['code'])
                facedevice = str(message['info']['facesluiceId'])
                deviceCode = facedevice.split("@@@")[1]
                workerCode = message['info']['customId']

                if code != "200":
                    code_detail = message['info']['detail']
                else:
                    code_detail = 'No error message'

                logger.debug("operation : "+operation)
                logger.debug("messageId : "+messageId)
                logger.debug("code : "+code)
                logger.debug("facedevice : "+deviceCode)
                logger.debug("workerCode : "+workerCode)

                data = {
                    "messageId": messageId,
                    "deviceCode": deviceCode,
                    "workerCode": workerCode,
                    "code": code,
                    "code_detail": code_detail
                }

                mydb = dbClient[dbName]
                mycol = mydb[transectiontb]
                myquery = {"messageId": messageId}
                transection = mycol.find(myquery)

                for t in transection:
                    sub_transection = t['transection']
                    oper = t['operation']

                    for sub_t in sub_transection:
                        topic = sub_t['topic']
                        logger.debug("topic : "+topic)
                        if topic == parent_topic+"/face/"+deviceCode:
                            logger.debug("topic : "+topic)

                            # update on transection table
                            query = {"messageId": messageId, "transection": {
                                "$elemMatch": {"topic": topic, "ackcode": "wating ack"}}}
                            newvalues = {"$set": {
                                "transection.$.ackcode": code, "transection.$.ackdetail": code_detail}}
                            isSuccess = alicloudDatabase.updateOneToDB(
                                transectiontb, query, newvalues)
                            log = {
                                "data": data,
                                "tasks": {
                                    "database": {
                                        "collection": transectiontb,
                                        "operation": "update",
                                        "query": query,
                                        "newvalue": newvalues,
                                        "success": isSuccess
                                    }
                                }
                            }

                            logs = str(log)
                            logger.info(logs)

                            if code == "200":
                                if oper == create_worker_operation_name:
                                    # create worker
                                    # update on workerlist table
                                    dtime = datetime.now()
                                    last_update = dtime.strftime(
                                        "%Y-%m-%d %H:%M:%S")
                                    query = {"registration.last_messageId": messageId, "devices": {
                                        "$elemMatch": {"deviceCode": deviceCode}}}
                                    newvalues = {"$set": {"registration.last_update": last_update,
                                                          "devices.$.regester": "registered", "devices.$.last_update": last_update}}
                                    isSuccess = alicloudDatabase.updateOneToDB(
                                        workertb, query, newvalues)
                                    log = {
                                        "data": data,
                                        "tasks": {
                                            "database": {
                                                "collection": workertb,
                                                "operation": "update",
                                                "query": query,
                                                "newvalue": newvalues,
                                                "success": isSuccess
                                            }
                                        }
                                    }

                                    logs = str(log)
                                    logger.info(logs)
                            

                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            
            else:
                # not DEVICE_ACK package , Do nothing
                logger.debug('not DEVICE_ACK package , Do nothing')
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

        except Exception as e:
            logger.error("Error on "+str(e)+", or Invalid message format -- drop message")
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    def run(self):
        try:
            logger.debug('starting thread to consume from rabbit...')
            self.channel.start_consuming()

        except Exception as e:
            logger.error(str(e))

def main():
    for i in range(THREADS):
        logger.debug('launch thread '+str(i))
        td = ThreadedConsumer()
        td.start()

if __name__ == "__main__":
    main()
