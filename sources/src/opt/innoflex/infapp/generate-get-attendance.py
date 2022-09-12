import string
import random
import configparser
from module import alicloudAMQP

config_path = "/opt/innoflex/config/configfile.ini"
config_obj = configparser.ConfigParser()
config_obj.read(config_path)
infamqp = config_obj["amqp"]
infqueue = config_obj["queue"]
infroute = config_obj["route"]

def randomString(length):
    letters_and_digits = string.ascii_lowercase + string.digits
    result_str = ''.join((random.choice(letters_and_digits) for i in range(length)))
    return result_str

messageId= randomString(8)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(4)+"-"+randomString(12)
msg = {
    "messageId": messageId,
    "operation": "GET_ATTENDANCE",
    "info": {
        "startTime": "2022-09-02 17:52:00",
        "endTime": "2022-09-02 17:53:00",
        "workerCodes": ["BG001"],
        "facilities": ["SSW"]
    }
}

queueName = infqueue['attendanceget']
exchange = infamqp['exchange']
route = str(infroute['attendanceget'])
routing_key = exchange+"."+route

alicloudAMQP.amqpPublish(exchange,routing_key,msg,queueName)
