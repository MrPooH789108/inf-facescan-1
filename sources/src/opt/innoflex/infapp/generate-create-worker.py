import string
import random
import configparser
from module import alicloudAMQP

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
infamqp = config_obj["amqp"]

def randomString(length):
    letters_and_digits = string.ascii_lowercase + string.digits
    result_str = ''.join((random.choice(letters_and_digits) for i in range(length)))
    return result_str

messageId= randomString(8)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(12)
msg = {
    "messageId": messageId,
    "operation": "CREATE_UPDATE_WORKER",
    "info": {
        "workerCode": "BG001",
        "name": "Pratchaya Kongchai",
        "gender": "FEMALE",
        "workerType": "DAILY_WORKER",
        "facilities": ["LKB","KSN"],
        "pictureURL": "https://share-bluegreen.s3.ap-southeast-1.amazonaws.com/pratchaya.jpg",
        "status": "INACTIVE"
    }
}

exchange = str(infamqp['exchange'])
routingKey = "amq.topic.workersync"
queueName = "LAZADA_WFM-WORKER-SYNC"

alicloudAMQP.amqpPublish(exchange,routingKey,msg,queueName)
