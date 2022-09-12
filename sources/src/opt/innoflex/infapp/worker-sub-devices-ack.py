""" This module receive delete person ack """

from module import alicloudDatabase
from module import connection
from pymongo import MongoClient
from datetime import datetime
import configparser
import threading
import logging
import pika
import ast
import sys
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infcollection = config_obj["collection"]
infbucket = config_obj["bucket"]
infdatabase = config_obj["db"]
inftopic = config_obj["topic"]
infqueue = config_obj["queue"]
inflog = config_obj["log"]
infetc = config_obj["etc"]
infamqp = config_obj["amqp"]
infroute = config_obj["route"]
infoperation = config_obj["operation"]

bucketName = infbucket['name']
folderName = infbucket['folder']
bucketURL = infbucket['url']

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

class ThreadedConsumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        connect = pika.BlockingConnection(connection.getConnectionParam())
        self.channel = connect.channel()
        self.channel.queue_declare(queueName, durable=True, auto_delete=False)
        self.channel.queue_bind(exchange=exchange,queue=queueName,routing_key=routing_key)
        self.channel.basic_qos(prefetch_count=THREADS*10)
        threading.Thread(target=self.channel.basic_consume(
            queueName, on_message_callback=self.on_message))

    def on_message(self, channel, method_frame, header_frame, body):
        try:
            print(method_frame.delivery_tag)
            body = str(body.decode())
            body = body.replace('\\r\\n', '')
            body = body.replace('\\', '')
            body = body[1:]
            body = body[:-1]
            print(body)
            print()

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

                print("operation : "+operation)
                print("messageId : "+messageId)
                print("code : "+code)
                print("facedevice : "+deviceCode)
                print("workerCode : "+workerCode)

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
                        print("topic : "+topic)
                        if topic == parent_topic+"/face/"+deviceCode:
                            print("topic : "+topic)

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

                print("operation : "+operation)
                print("messageId : "+messageId)
                print("code : "+code)
                print("facedevice : "+deviceCode)
                print("workerCode : "+workerCode)

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
                        print("topic : "+topic)
                        if topic == parent_topic+"/face/"+deviceCode:
                            print("topic : "+topic)

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
                channel.basic_reject(delivery_tag=method_frame.delivery_tag)

        except Exception as e:
            print(str(e))
            log = {
                "data": data,
                "error": str(e)
            }
            logs = str(log)
            logger.error(logs)
            channel.basic_reject(delivery_tag=method_frame.delivery_tag)

    def run(self):
        try:
            print('starting thread to consume from rabbit...')
            self.channel.start_consuming()

        except Exception as e:
            print(str(e))


def main():
    for i in range(THREADS):
        print('launch thread '+str(i))
        td = ThreadedConsumer()
        td.start()


if __name__ == "__main__":
    #Creating and Configuring Logger
    logger = logging.getLogger('receieveDeviceAck')
    fileHandler = logging.FileHandler(LOG_PATH+"/inf-worker-sync.log")
    streamHandler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "function": "%(funcName)s", "message": "%(message)s"}')
    streamHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)
    logger.addHandler(fileHandler)
    logger.setLevel(logging.DEBUG)

    #reduce pika log level
    logging.getLogger("pika").setLevel(logging.WARNING)
    main()
