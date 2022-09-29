from pymongo import MongoClient
import configparser
import logging
import sys
import os


config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
infdatabase = config_obj["db"]
infcollection = config_obj["collection"]
inflog = config_obj["log"]

dbUser = str(os.environ['DB_USER'])
dbPass = str(os.environ['DB_PASS'])

dbHost = "mongodb://"+infdatabase['nodes']
dbReplicaSet = infdatabase['replicaSet']
dbClient = MongoClient(host=dbHost, replicaset=dbReplicaSet, username=dbUser,
                       password=dbPass, authSource='admin', authMechanism='SCRAM-SHA-256')

dbName = infdatabase['name']
devicetb = infcollection['devices']

LOG_PATH = inflog['path']

#Creating and Configuring Logger
logger = logging.getLogger('database')

fileFormat = logging.Formatter('{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "message": "%(message)s"}')
fileHandler = logging.FileHandler(LOG_PATH+"/database.log")
fileHandler.setFormatter(fileFormat)
fileHandler.setLevel(logging.INFO)

streamFormat = logging.Formatter('%(asctime)s %(name)s [%(levelname)s] %(message)s')
streamHandler = logging.StreamHandler(sys.stdout)
streamHandler.setFormatter(streamFormat)
streamHandler.setLevel(logging.DEBUG)

#reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)


def insertToDB(tbName, msg):
    isSuccess = False
    db={}
    mydb = dbClient[dbName]
    mycol = mydb[tbName]
    dblist = dbClient.list_database_names()

    db['name'] = dbName
    db['collection']=tbName
    db['operation'] = 'insert'
    db['info'] = msg

    try:
        if dbName in dblist:
            db['db_exists'] = True

        else:
            db['db_exists'] = False

        collist = mydb.list_collection_names()
        if tbName in collist:
            result = mycol.insert_one(msg)
            db['collection_exist']=True
            db['collection_new_create']=False

        else:
            result = mycol.insert_one(msg)
            db['collection_exist']=False
            db['collection_new_create']=True

        db['result_id'] = str(result.inserted_id)
        db['result_ack'] = str(result.acknowledged)

        isSuccess = True
        db['isSuccess'] = isSuccess
        log = {}
        log["database"]=db
        logger.info(log)
        
        return isSuccess

    except Exception as e:
        isSuccess = False
        db['isSuccess'] = isSuccess
        db['error'] = str(e)
        log = {}
        log["database"]=db
        logger.error(log)
        return isSuccess


def getAlldevicesByfacility(facilities):
    try:
        db={}
        mydb = dbClient[dbName]
        mycol = mydb[devicetb]
        sub_query = []

        db['name'] = dbName
        db['collection']=devicetb
        db['operation'] = 'find'

        for fac in facilities:
            q = {"facility": fac}
            sub_query.append(q)
        
        myquery = {"$or": sub_query}
        db['query'] = myquery

        devices = mycol.find(myquery)
        db['result'] = devices

        log = {}
        log["database"]=db
        logger.info(log)
        return devices

    except Exception as e:
        devices =[]
        logger.error(str(e))
        return devices


def updateOneToDB(tbName, filter, newvalues):
    try:
        db={}
        mydb = dbClient[dbName]
        collection = mydb[tbName]

        db['name'] = dbName
        db['collection']=tbName
        db['operation'] = 'update'
        db['query'] = filter
        db['newvalue'] = newvalues

        # Using update_one() method for single updation.
        logger.debug("Using update_one() method for single updation")
        result = collection.update_one(filter, newvalues)

        db['result_ack'] = str(result.acknowledged)

        # Printing the updated content of the database
        logger.debug("Printing the updated content of the database")
        cursor = collection.find(filter)
        for record in cursor:
            logger.debug(record)

        log = {}
        log["database"]=db
        logger.info(log)
        return True

    except Exception as e:
        logger.error(str(e))
        return False


def deleteOneFromDB(tbName, query):
    try:
        db={}
        mydb = dbClient[dbName]
        mycol = mydb[tbName]

        db['name'] = dbName
        db['collection']=tbName
        db['operation'] = 'delete'
        db['query'] = query

        result =  mycol.delete_one(query)
        count = result.deleted_count

        db['result_ack'] = str(result.acknowledged)
        db['result_count'] = count

        return True

    except Exception as e:
        logger.error(str(e))
        return False
