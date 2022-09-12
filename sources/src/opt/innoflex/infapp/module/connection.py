import configparser
import pika
import oss2
import os

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
infamqp = config_obj["amqp"]

accessKey = str(os.environ['OSS_ACCESS_KEY'])
secretKey = str(os.environ['OSS_SECRET_KEY'])

username=str(os.environ['AMQP_USER'])
password=str(os.environ['AMQP_PASS'])

amqp_host = infamqp['endpoint']
amqp_port = infamqp['port']
virtualHost = infamqp['virtualHost']
amqp_instanceId = infamqp['instanceId']



def getConnectionParam():
    credentials = pika.PlainCredentials(
        username, password, erase_on_connect=True)
    return pika.ConnectionParameters(amqp_host, amqp_port, virtualHost, credentials)


def getBucketConnection(bucketURL, bucketName):
    auth = oss2.Auth(accessKey, secretKey)
    bucket = oss2.Bucket(auth, bucketURL, bucketName)
    return bucket
