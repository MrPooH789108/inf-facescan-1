""" This module recieve attendance message from devices and send attendance to wfm """
""" attendance-sync.py """

import os
import sys
import ast
import pika
import string
import random
import logging
import threading
import configparser
from pymongo import MongoClient
from module import alicloudDatabase
from module import connection
from module import alicloudAMQP

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
infdatabase = config_obj["db"]
infcollection = config_obj["collection"]
infqueue = config_obj["queue"]
inftopic = config_obj["topic"]
inflog = config_obj["log"]
infetc = config_obj["etc"]
infamqp = config_obj["amqp"]
infroute = config_obj["route"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+str(infdatabase['nodes'])
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
devicetb = infcollection['devices']
attendancetb = infcollection['attendances']
blacklistlogtb = infcollection['blacklists']

queueName = infqueue['devicerec']
exchange = infamqp['exchange']
route = str(infroute['devicerec'])
routing_key = exchange+"."+route

LOG_PATH = inflog['path']
THREADS = int(infetc['threadnum'])

logger = logging.getLogger('AttendanceSync')
logger.setLevel(logging.DEBUG)

fileFormat = logging.Formatter('{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')
fileHandler = logging.FileHandler(LOG_PATH+"/inf-attendance-sync.log")        
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

class SyncAttendanceHandler(threading.Thread):
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
            def randomString(length):
                letters_and_digits = string.ascii_lowercase + string.digits
                result_str = ''.join(
                    (random.choice(letters_and_digits) for i in range(length)))
                #logger.debug("Random alphanumeric String is:", result_str)
                return result_str

            logger.debug(method_frame.delivery_tag)
            body = str(body.decode())
            body = body.replace('\\r\\n', '')
            body = body.replace('\\', '')
            body = body[1:]
            body = body[:-1]
            logger.debug(body)

            message = ast.literal_eval(body)
            operator = message["operator"]

            if operator == "RecPush": 
                facedevice = message["info"]["facesluiceId"]
                deviceCode = facedevice.split("@@@")[1]
                logger.debug("deviceCode : "+deviceCode)

                workerCode = message["info"]["customId"]
                if workerCode == " ":
                    workerCode = message["info"]["idCard"]

                logger.debug("workerCode : "+workerCode)

                personType = message["info"]["PersonType"]

                mydb = dbClient[dbName]
                mycol = mydb[devicetb]

                myquery = {"deviceCode": deviceCode}
                devices = mycol.find(myquery)

                facility = ""
                for device in devices:
                    logger.debug(device)
                    facility = str(device["facility"])

                messageId = randomString(
                    8)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(12)
                logger.debug("messageId : "+messageId)

                attendanceTime = str(message["info"]["time"])
                attendanceDate = attendanceTime.replace("/","-")
                logger.debug("Timestamp : "+ str(attendanceDate))

                direction = message["info"]["direction"]  # exit or entr

                if direction == "entr":
                    d_type = "IN"
                    logger.debug("type : "+d_type)
                elif direction == "exit":
                    d_type = "OUT"
                    logger.debug("type : "+d_type)

                syncAttendance_json = {
                    "_id": messageId,
                    "messageId": messageId,
                    "operation": "SYNC_ATTENDANCE",
                    "info": {
                        "workerCode": workerCode,
                        "deviceCode": deviceCode,
                        "facility": facility,
                        "attendanceDate": attendanceDate,
                        "type": d_type
                    }
                }

                if personType == "0":  # whitelist
                    routing = exchange+"."+str(infroute['attendancesync'])
                    queue = str(infqueue['attendancesync'])
                    isqmqpSuccess = alicloudAMQP.amqpPublish(exchange,routing,syncAttendance_json,queue)
                    isSuccess = alicloudDatabase.insertToDB(
                        attendancetb, syncAttendance_json)

                    log = {
                        "data": syncAttendance_json,
                        "tasks": {
                            "amqp": {
                                "queue": queue,
                                "success": isqmqpSuccess
                            },
                            "database": {
                                "collection": attendancetb,
                                "success": isSuccess
                            }
                        }
                    }

                    logger.debug("Insert attendance log success ? : "+ str(isSuccess))

                if personType == "1":  # blacklist
                    isSuccess = alicloudDatabase.insertToDB(
                        blacklistlogtb, syncAttendance_json)

                    log = {
                        "data": syncAttendance_json,
                        "tasks": {
                            "database": {
                                "collection": blacklistlogtb,
                                "success": isSuccess
                            }
                        }
                    }
                    logger.debug("Insert attendance log success ? : "+str(isSuccess))

                logs = str(log)
                logger.info(logs.replace("'",'"'))
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            
            else:
                # not RecPush package , Do nothing
                logger.debug('not RecPush package , Do nothing')
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
        td = SyncAttendanceHandler()
        td.start()


if __name__ == "__main__":
    main()
