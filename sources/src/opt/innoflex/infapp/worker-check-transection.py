""" This module check complete transection and send res message to wfm """
""" worker-check-transection.py """

from module import alicloudDatabase
from module import alicloudAMQP
from pymongo import MongoClient
import configparser
import threading
import logging
import socket
import sys
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infsecinterval = config_obj["interval_sec"]
infcollection = config_obj["collection"]
infoperation = config_obj["operation"]
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
transectiontb = infcollection['transections']

# run every 10 mins = 600 seconds
transection_review_interval = int(infsecinterval['transection_review'])
create_worker_operation_name = infoperation['create_worker']
change_status_operation_name = infoperation['change_status']

appname = socket.gethostname()+'_checkTransction'

parent_topic = inftopic['parent']
sub_topic = inftopic['workerSyncRes']
pub_topic = parent_topic+"/"+sub_topic

exchange = str(infamqp['exchange'])

# run every 30 mins = 1800 seconds
transection_timeout = int(infsecinterval['transection_timeout'])
timeout = int(transection_timeout/transection_review_interval)

LOG_PATH = inflog['path']

#Creating and Configuring Logger
logger = logging.getLogger('CheckTransection')
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

# check if all elements in a List are same
def chkList(lst):
    return len(set(lst)) == 1

def checkTransectionAndSendREStoLAZ():
    try:
        threading.Timer(transection_review_interval,
                        checkTransectionAndSendREStoLAZ).start()

        mydb = dbClient[dbName]
        mycol = mydb[transectiontb]
        myquery = {"recieveAllack": False,"timeout": False}
        transections = mycol.find(myquery)
        
        for t in transections:
            notAck = 0
            messageId = t['messageId']
            operation = t['operation']
            sub_transection = t['transection']
            all_ackdetail = []
            all_ackcode = []
            for sub_t in sub_transection:
                ackcode = sub_t['ackcode']
                all_ackcode.append(ackcode)
                if ackcode != "wating ack":
                    ackdetail = sub_t['ackdetail']
                    all_ackdetail.append(ackdetail)
                else:
                    notAck = notAck+1

            if notAck == 0:
                if chkList(all_ackcode) == True:
                    ackcode = all_ackcode[0]
                    all_ackdetail = all_ackdetail[0]
                    logger.debug("All result same ackcode : "+str(ackcode))

                else:
                    all_ackcode.remove('200')
                    all_ackdetail.remove('No error message')
                    ackcode = all_ackcode[0]
                    all_ackdetail = all_ackdetail[0]
                    logger.debug("All result diff ackcode")

                # update on transection table
                query = {"messageId": messageId}
                newvalues = {"$set": {"recieveAllack": True}}
                isUpdate = alicloudDatabase.updateOneToDB(
                    transectiontb, query, newvalues)

                # send RES to LAZ
                if ackcode == "200":
                    errMsg = "No error message"
                else:
                    errMsg = "Failed to create/update worker due to.. "+ackdetail

                if operation == create_worker_operation_name:
                    message = {
                        "messageId": messageId,
                        "operation": create_worker_operation_name+"_RES",
                        "code": ackcode,
                        "errorMsg": errMsg,
                        "info": t['info']
                    }

                elif operation == change_status_operation_name:
                    message = {
                        "messageId": messageId,
                        "operation": change_status_operation_name+"_RES",
                        "code": ackcode,
                        "errorMsg": errMsg,
                        "info": t['info']
                    }

                routingKey = exchange+"."+str(infroute['workersyncres'])
                queueName = str(infqueue['workersyncres'])
                isqmqpSuccess = alicloudAMQP.amqpPublish(exchange,routingKey,message,queueName)
                log = {
                    "data": message,
                    "tasks": {
                        "amqp": {
                            "queue": queueName,
                            "success": isqmqpSuccess
                        },
                        "database": {
                            "collection": transectiontb,
                            "operation": "update",
                            "query": query,
                            "newvalue": newvalues,
                            "success": isUpdate
                        }
                    }
                }
                logs = str(log)
                logger.info(logs.replace("'", '"'))

            else:
                recheck = t['recheck']
                queueName = str(infqueue['workersyncres'])
                if recheck < timeout:
                    recheck = recheck+1
                    query = {"messageId": messageId}
                    newvalues = {"$set": {"recheck": recheck}}
                    isUpdate = alicloudDatabase.updateOneToDB(
                        transectiontb, query, newvalues)
                    logger.debug("timeout : "+str(timeout))
                    logger.debug("recheck : "+str(recheck))
                    log = {
                        "data": {
                            "messageId": messageId,
                            "info": t['info'],
                            "timeout": False,
                            "recheck": recheck
                        },
                        "tasks": {
                            "amqp": {
                                "queue": queueName,
                                "success": False
                            },
                            "database": {
                                "collection": transectiontb,
                                "operation": "update",
                                "query": query,
                                "newvalue": newvalues,
                                "success": isUpdate
                            }
                        }
                    }
                    logs = str(log)
                    logger.info(logs.replace("'", '"'))

                else:
                    query = {"messageId": messageId}
                    newvalues = {"$set": {"timeout": True}}
                    isUpdate = alicloudDatabase.updateOneToDB(
                        transectiontb, query, newvalues)
                    logger.debug("Waiting ack from device timeout.")
                    message = {
                        "messageId": messageId,
                        "operation": "CREATE_UPDATE_WORKER_RES",
                        "code": 409,
                        "errorMsg": "Failed to create/update worker due to waiting ack from device timeout.",
                        "info": t['info']
                    }
                    routingKey = exchange+"."+str(infroute['workersyncres'])
                    queueName = str(infqueue['workersyncres'])
                    isqmqpSuccess = alicloudAMQP.amqpPublish(exchange,routingKey,message,queueName)
                    log = {
                        "data": message,
                        "tasks": {
                            "amqp": {
                                "queue": queueName,
                                "success": isqmqpSuccess
                            },
                        }
                    }
                    logs = str(log)
                    logger.info(logs.replace("'", '"'))

    except Exception as e:
        logger.error(str(e))

checkTransectionAndSendREStoLAZ()
