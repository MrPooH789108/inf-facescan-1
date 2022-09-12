from module import connection
import logging
import json
import pika
import sys

#Creating and Configuring Logger
logger = logging.getLogger('AMQP-Handler')
#fileHandler = logging.FileHandler(LOG_PATH)
streamHandler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('{"timestamp":"%(asctime)s", "name": "%(name)s", "level": "%(levelname)s", "function": "%(funcName)s", "message": "%(message)s"}')
streamHandler.setFormatter(formatter)
#fileHandler.setFormatter(formatter)
logger.addHandler(streamHandler)
#logger.addHandler(fileHandler)
logger.setLevel(logging.DEBUG)

#reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

def on_message(channel, method_frame, header_frame, body):
    #logger.debug(method_frame.delivery_tag)
    logger.debug(body)
    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

def amqpPublish(exchangeName,routingKey,message,queueName):
    try:
        logger.debug("Connecting AMQP Broker...")
        #connect
        connect = pika.BlockingConnection(connection.getConnectionParam())
        channel = connect.channel()
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(queueName, durable = True, auto_delete = False)

        #send message
        logger.debug("Publishing Message...")
        data_out=json.dumps(message)
        properties = pika.spec.BasicProperties(content_type = "application/json", delivery_mode = 2)
        channel.basic_publish(exchange = exchangeName, routing_key = routingKey, body = data_out, properties = properties)
        
        logger.debug("Disconnect...")
        #disconnect
        connect.close()
        return "Publish Success !"

    except Exception as e:
        if str(e) == "[Errno -2] Name or service not known":
            logger.error("Can't connect with AMQP Broker , please check endpoint name in config file.")
        else:
            logger.error(str(e))
