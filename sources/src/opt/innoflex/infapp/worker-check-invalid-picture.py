"""This module check if picture of worker is already in oss folder"""

from module import alicloudDatabase
from module import alicloudAMQP
from module import connection
from datetime import datetime
from pymongo import MongoClient
import configparser
import threading
import logging
import socket
import oss2
import sys
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infsecinterval = config_obj["interval_sec"]
infdayinterval = config_obj["interval_day"]
infcollection = config_obj["collection"]
infbucket = config_obj["bucket"]
infdatabase = config_obj["db"]
inftopic = config_obj["topic"]
inflog = config_obj["log"]
infamqp = config_obj["amqp"]
infroute = config_obj["route"]
infqueue = config_obj["queue"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
workertb = infcollection['workers']

bucketName = infbucket['name']
folderName = infbucket['folder']
bucketURL = infbucket['url']

# 1 day = 1440 mins
picture_waiting_interval = int(infdayinterval['picture_waiting'])

# run every 5 mins = 300 seconds
picture_review_interval = int(infsecinterval['picture_review'])

# recheck every 5 mins
timeout = int((picture_waiting_interval * 1440)/5)

exchange = str(infamqp['exchange'])

parent_topic = inftopic['parent']
sub_topic = inftopic['workerSyncRes']
pub_topic = parent_topic+"/"+sub_topic

LOG_PATH = inflog['path']

#Creating and Configuring Logger
logger = logging.getLogger('checkPictureInOSS')
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

def checkInvalidPicture():
    threading.Timer(picture_review_interval, checkInvalidPicture).start()
    try:

        mydb = dbClient[dbName]
        mycol = mydb[workertb]

        myquery = {"picture.status": "invalid"}
        workers = mycol.find(myquery)

        for w in workers:
            print(w)
            bucket = connection.getBucketConnection(bucketURL, bucketName)
            messageId = w["registration"]["last_messageId"]
            picPath = w['picture']['path']
            image_recheck = int(w['picture']['recheck'])
            print("messageId : "+messageId)
            print("picPath : "+picPath)

            # update time
            dtime = datetime.now()
            last_update = dtime.strftime("%Y-%m-%d %H:%M:%S")

            foundObj = "invalid"
            for obj in oss2.ObjectIterator(bucket, prefix=picPath):
                foundObj = "valid"
                print("found Obj : "+str(obj.key))

            # Can't find picture in OSS
            if foundObj == "invalid":
                print("time out recheck : "+str(timeout))
                image_recheck = image_recheck+1
                print("recheck : "+str(image_recheck))

                if image_recheck < timeout:
                    query = {"registration.last_messageId": messageId}
                    newvalues = {
                        "$set": {'picture.recheck': image_recheck, 'picture.last_update': last_update}}
                    isSuccess = alicloudDatabase.updateOneToDB(
                        workertb, query, newvalues)
                    log = {
                        "data": {
                            "picPath": picPath,
                            "status": foundObj,
                            "timeout": False,
                            "recheck": image_recheck
                        },
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

                else:
                    query = {"registration.last_messageId": messageId}
                    print("Waiting picture for '"+picPath+"' timeout.")
                    message = {
                        "messageId": messageId,
                        "operation": "CREATE_UPDATE_WORKER_RES",
                        "code": 408,
                        "errorMsg": "Failed to create/update worker due to can't find picture '"+picPath+"', request timeout",
                        "info": w['info']
                    }
                    print(message)

                    print("Invalid picture timeout, delete workerlist")

                    routingKey = exchange+"."+str(infroute['workersyncres'])
                    queueName = str(infqueue['workersyncres'])
                    isqmqpSuccess = alicloudAMQP.amqpPublish(exchange,routingKey,message,queueName)

                    log = {
                        "data": {
                            "picPath": picPath,
                            "status": foundObj,
                            "timeout": True,
                            "recheck": image_recheck
                        },
                        "tasks": {
                            "amqp": {
                                "queue": queueName,
                                "success": isqmqpSuccess
                            },
                            "database": {
                                "collection": workertb,
                                "operation": "delete",
                                "query": query,
                                "success": isSuccess
                            }
                        }
                    }

                logs = str(log)
                logger.info(logs.replace("'", '"'))

            elif foundObj == "valid":
                query = {"registration.last_messageId": messageId}
                newvalues = {
                    "$set": {'picture.status': "valid", 'picture.last_update': last_update}}
                isSuccess = alicloudDatabase.updateOneToDB(
                    workertb, query, newvalues)
                log = {
                    "data": {
                        "picPath": picPath,
                        "status": foundObj,
                        "timeout": False,
                        "recheck": image_recheck
                    },
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
                logger.info(logs.replace("'", '"'))

    except Exception as e:
        print(str(e))
        log = {
            "data": message,
            "error": str(e)
        }
        logs = str(log)
        logger.error(logs.replace("'", '"'))


checkInvalidPicture()
