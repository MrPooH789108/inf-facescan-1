""" This module receive create worker """

from module import connection
from module import alicloudDatabase
from datetime import datetime
from pymongo import MongoClient
import configparser
import threading
import logging
import pika
import oss2
import ast
import sys
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)

infcollection = config_obj["collection"]
infqueue = config_obj["queue"]
infetc = config_obj["etc"]
inflog = config_obj["log"]
infamqp = config_obj["amqp"]
infroute = config_obj["route"]
infdatabase = config_obj["db"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
workertb = infcollection['workers']

queueName = infqueue['workerSync']
exchange = infamqp['exchange']
route = str(infroute['workersync'])
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
            print(body)
            print()

            message = ast.literal_eval(body)
            operation = message['operation']
            print("operation : "+operation)
            if operation == "CREATE_UPDATE_WORKER":
                info = message['info']
                messageId = message['messageId']
                workerCode = message['info']['workerCode']
                facilities = message['info']['facilities']
                status = message['info']['status']
                pictureURL = message['info']['pictureURL']

                time = datetime.now()
                last_update = time.strftime("%Y-%m-%d %H:%M:%S")
                print("Data formating:", last_update)

                # find existing workercode
                mydb = dbClient[dbName]
                mycol = mydb[workertb]
                myquery = {"_id":workerCode}
                w_exist = mycol.find(myquery)
                w_count = 0
                for w in w_exist:
                    w_count = w_count+1

                print("Existing worker : "+ str(w_count))
                # Exising worker
                if w_count > 0 :
                    print("worker code is already exist")
                    w_exist = mycol.find(myquery)
                    for w in w_exist:
                        print(w)
                        # update info section
                        newvalues = {"$set": {"info": info}}
                        isUpdateInfo = alicloudDatabase.updateOneToDB(
                            workertb, myquery, newvalues)

                        #update registration section
                        registration = {
                            "last_messageId": messageId,
                            "last_update": last_update
                        }
                        newvalues = {"$set": {"registration": registration}}
                        isUpdateRegis = alicloudDatabase.updateOneToDB(
                            workertb, myquery, newvalues)

                        # update devices section
                        print("--- update devices section ")
                        old_devices = w["devices"]
                        
                        # list exist facilities
                        print("--- list exist facilities ")
                        old_faci = []
                        for ex in old_devices:
                            facility = ex["facility"]
                            old_faci.append(facility)
                        
                        old_faci = list(dict.fromkeys(old_faci))
                        print("old_faci : "+str(old_faci))

                        new_faci = facilities
                        print("new_faci : "+str(new_faci))

                        add_faci = list(set(new_faci) - set(old_faci)) # add
                        del_faci = list(set(old_faci) - set(new_faci)) # del

                        print("Add new facility : "+str(add_faci))
                        print("Del old facility : "+str(del_faci))


                        # update new devices 
                        all_devices=old_devices
                        if len(add_faci) > 0 : 
                            devices = alicloudDatabase.getAlldevicesByfacility(add_faci)
                            for d in devices:
                                device = d
                                del device["_id"]
                                del device["name"]
                                del device["ipaddr"]
                                # print(d)
                                device["status"] = status
                                device["regester"] = "unregistered"
                                device["last_update"] = last_update

                                all_devices.append(device)
                            
                            print("--- all devices ---")
                            print(all_devices)
                            newvalues = {"$set": {"devices": all_devices}}
                            isUpdateDevices = alicloudDatabase.updateOneToDB(
                                workertb, myquery, newvalues)
                        else:
                            isUpdateDevices = False

                        # update old devices with new status
                        if len(old_faci) > 0 : 
                            devices = alicloudDatabase.getAlldevicesByfacility(old_faci)
                            for d in devices:
                                deviceCode = d['deviceCode']
                                print("device code : "+str(deviceCode))
                                query = {"info.workerCode": workerCode, "devices": {
                                    "$elemMatch": {"deviceCode": deviceCode}}}

                                if status == "BLACKLISTED":
                                    print("query : "+str(query))
                                    newvalues = {"$set": {"devices.$.status": "BLACKLISTED","devices.$.regester": "unregistered", "devices.$.last_update": last_update}}
                                
                                else:
                                    print("query : "+str(query))
                                    newvalues = {"$set": {"devices.$.status": status, "devices.$.last_update": last_update}}

                                print("new value : "+str(newvalues))
                                isUpdateDevices = alicloudDatabase.updateOneToDB(
                                    workertb, query, newvalues)
                        else:
                            isUpdateDevices = False

                        # update del devices with inactive status
                        if len(del_faci) > 0 : 
                            devices = alicloudDatabase.getAlldevicesByfacility(del_faci)
                            for d in devices:
                                deviceCode = d['deviceCode']
                                print("device code : "+str(deviceCode))
                                query = {"info.workerCode": workerCode, "devices": {
                                    "$elemMatch": {"deviceCode": deviceCode}}}
                                print("query : "+str(query))
                                newvalues = {"$set": {"devices.$.status": "INACTIVE", "devices.$.regester": "registered", "devices.$.last_update": last_update}}
                                print("new value : "+str(newvalues))
                                isUpdateDevices = alicloudDatabase.updateOneToDB(
                                    workertb, query, newvalues)
                        else:
                            isUpdateDevices = False

                        log = {
                            "data": message,
                            "tasks": {
                                "database": {
                                    "collection": workertb,
                                    "update": {
                                        "info": isUpdateInfo,
                                        "registration": isUpdateRegis,
                                        "devices": isUpdateDevices
                                    }
                                }
                            }
                        }

                        logs = str(log)
                        logger.info(logs.replace("'", '"'))

                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                        

                # new worker
                else:
                    # registration section
                    registration = {
                        "last_messageId": messageId,
                        "last_update": last_update
                    }

                    # devices section
                    all_devices = []
                    devices = alicloudDatabase.getAlldevicesByfacility(facilities)
                    for d in devices:
                        device = d
                        del device["_id"]
                        del device["name"]
                        del device["ipaddr"]
                        # print(d)
                        device["status"] = status
                        device["regester"] = "unregistered"
                        device["last_update"] = last_update

                        all_devices.append(device)
                        print(all_devices)

                    del message["messageId"]
                    del message["operation"]

                    message["registration"] = registration
                    message["devices"] = all_devices
                    message["pictureURL"] = pictureURL
                    message["_id"] = workerCode

                    print(message)
                    isSuccess = alicloudDatabase.insertToDB(workertb, message)
                    print("insert data to database success ? : "+str(isSuccess))

                    log = {
                        "data": message,
                        "tasks": {
                            "database": {
                                "collection": workertb,
                                "create": isSuccess
                            }
                        }
                    }

                    logs = str(log)
                    logger.info(logs.replace("'", '"'))

                    if isSuccess == False:
                        channel.basic_reject(
                            delivery_tag=method_frame.delivery_tag)
                    else:
                        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            
        except Exception as e:
            print(str(e))
            log = {
                "data": message,
                "error": str(e)
            }
            logs = str(log)
            logger.error(logs.replace("'", '"'))
            channel.basic_reject(delivery_tag=method_frame.delivery_tag)

    def run(self):
        try:
            logger.debug('starting thread to consume from AMQP...')
            self.channel.start_consuming()

        except Exception as e:
            print(str(e))


def main():
    for i in range(THREADS):
        logger.debug('launch thread '+str(i))
        td = ThreadedConsumer()
        td.start()


if __name__ == "__main__":
    #Creating and Configuring Logger
    logger = logging.getLogger('receieveCreateWorker')
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
