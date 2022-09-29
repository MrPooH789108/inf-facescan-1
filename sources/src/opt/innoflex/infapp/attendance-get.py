""" This module recieve get-attendance message and send attendance history to wfm """
""" attendance-get.py"""

import os
import sys
import ast
import pika
import logging
import threading
import configparser
from pymongo import MongoClient
from module import alicloudDatabase
from module import alicloudAMQP
from module import connection

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
infdatabase = config_obj["db"]
infcollection = config_obj["collection"]
infqueue = config_obj["queue"]
inftopic = config_obj["topic"]
inflog = config_obj["log"]
infetc = config_obj["etc"]
infoperation = config_obj["operation"]
infamqp = config_obj["amqp"]
infroute = config_obj["route"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
devicetb = infcollection['devices']
attendancetb = infcollection['attendances']
transectiontb = infcollection['transections']

get_attendance_operation_name = infoperation['get_attendance']

parent_topic = inftopic['parent']
QUEUENAME = infqueue['attendanceget']
EXCHANGE = infamqp['exchange']
route = str(infroute['attendanceget'])
ROUTING_KEY = EXCHANGE+"."+route

LOG_PATH = inflog['path']
THREADS = int(infetc['threadnum'])

logger = logging.getLogger('GetAttendance')
logger.setLevel(logging.DEBUG)

fileFormat = logging.Formatter('{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')
fileHandler = logging.FileHandler(LOG_PATH+"/inf-attendance-get.log")        
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
        self.channel.queue_declare(QUEUENAME, durable=True, auto_delete=False)
        self.channel.basic_qos(prefetch_count=THREADS*10)
        threading.Thread(target=self.channel.basic_consume(
            QUEUENAME, on_message_callback=self.on_message))

    def on_message(self, channel, method_frame, header_frame, body):
        try:
            logger.debug(method_frame.delivery_tag)
            body = str(body.decode())
            body = body.replace("null",'""')
            logger.debug(body)

            message = ast.literal_eval(body)
            messageId = message["messageId"]
            operation = message["operation"]

            if operation == "GET_ATTENDANCE":

                startTime = str(message["info"]["startTime"])
                endTime = str(message["info"]["endTime"])

                workerCodes = message["info"]["workerCodes"]
                facilities = message["info"]["facilities"]

                mydb = dbClient[dbName]
                mycol = mydb[attendancetb]

                if workerCodes == "" and facilities == "":
                    all_attendances = []
                    myquery = {
                        "info.attendanceDate": {"$gte": startTime,
                                                "$lte": endTime
                                                }
                    }
                    logger.debug("queury : "+str(myquery))
                    attendances = mycol.find(myquery)

                    for a in attendances:
                        # logger.debug(a['info'])
                        all_attendances.append(a['info'])

                elif workerCodes != "" and facilities == "":
                    all_attendances = []
                    for code in workerCodes:
                        logger.debug("code : "+code)
                        myquery = {
                            "info.workerCode": code,
                            "info.attendanceDate": {"$gte": startTime,
                                                    "$lte": endTime
                                                    }
                        }

                        logger.debug("queury : "+str(myquery))
                        attendances = mycol.find(myquery)

                        for a in attendances:
                            # logger.debug(a['info'])
                            all_attendances.append(a['info'])

                elif workerCodes == "" and facilities != "":
                    all_attendances = []
                    for fac in facilities:
                        logger.debug("fac : "+fac)
                        myquery = {
                            "info.facility": fac,
                            "info.attendanceDate": {"$gte": startTime,
                                                    "$lte": endTime
                                                    }
                        }
                        logger.debug("queury : "+str(myquery))
                        attendances = mycol.find(myquery)

                        for a in attendances:
                            # logger.debug(a['info'])
                            all_attendances.append(a['info'])


                elif workerCodes != "" and facilities != "":
                    all_attendances = []
                    for code in workerCodes:
                        logger.debug("code : "+code)
                        for fac in facilities:
                            logger.debug("fac : "+fac)
                            myquery = {
                                "info.workerCode": code,
                                "info.facility": fac,
                                "info.attendanceDate": {"$gte": startTime,
                                                        "$lte": endTime
                                                        }
                            }
                            logger.debug("queury : "+str(myquery))
                            attendances = mycol.find(myquery)

                            for a in attendances:
                                # logger.debug(a['info'])
                                all_attendances.append(a['info'])

                if len(all_attendances) != 0:
                    errcode = "200"
                    msg_ack = {
                        "messageId": messageId,
                        "operation": get_attendance_operation_name+"_RES",
                        "code": errcode,
                        "errorMsg": "No error message",
                        "info": all_attendances
                    }
                    logger.debug("Get attendances Success !")

                else:
                    errcode = "404"
                    msg_ack = {
                        "messageId": messageId,
                        "operation": get_attendance_operation_name+"_RES",
                        "code": errcode,
                        "errorMsg": "Failed to get attendances due to... can't find any attendance",
                        "info": [
                        ]
                    }
                    logger.debug("Failed to get attendances due to... can't find any attendance")

                routingKey = EXCHANGE+"."+str(infroute['attendanceres'])
                queueName = str(infqueue['attendanceres'])
                isqmqpSuccess = alicloudAMQP.amqpPublish(
                    EXCHANGE, routingKey, msg_ack, queueName)

                all_transection = []
                transection = {}
                transection["topic"] = get_attendance_operation_name+"_RES"
                transection["body"] = msg_ack
                transection["ackcode"] = errcode

                all_transection.append(transection)

                data = {
                    "_id": messageId,
                    "messageId": messageId,
                    "operation": operation,
                    "info": message['info'],
                    "transection": all_transection,
                    "recieveAllack": True,
                    "recheck": 0,
                    "timeout": False
                }

                isSuccess = alicloudDatabase.insertToDB(transectiontb, data)

                logger.debug("Insert transection log success ? : " + str(isSuccess))
                log = {
                    "data": data,
                    "tasks": {
                        "amqp": {
                            "queue": queueName,
                            "routingKey": routingKey,
                            "success": isqmqpSuccess
                        },
                        "database": {
                            "collection": transectiontb,
                            "success": isSuccess
                        }
                    }
                }
                logs = str(log)
                logger.info(logs.replace("'", '"'))
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)

            else:
                # not GET_ATTENDANCE package , Do nothing
                logger.debug('not GET_ATTENDANCE package , Do nothing')
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
