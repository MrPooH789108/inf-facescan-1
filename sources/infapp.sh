#!/bin/sh

source /env_secrets_expand.sh

echo "... check Alicloud access key ..."
echo "ALI_ACCESS_KEY: ${ALI_ACCESS_KEY}"
echo "ALI_SECRET_KEY: ${ALI_SECRET_KEY}"

echo "... check Database user ..."
echo "DB_USER: ${DB_USER}"
echo "DB_PASS: ${DB_PASS}"

cd /opt/innoflex

echo "... complie code ..."
python3 -m compileall ./infapp

find -name "*.cpython-*.pyc*" -exec sh -c 'f="{}"; mv -- "$f" "${f%.cpython-*.pyc}.pyc"' \;

mv  -v ./infapp/__pycache__/* ./infapp/
mv  -v ./infapp/module/__pycache__/* ./infapp/module

echo "... remove source code ..."
rm -f infapp/*.py
rm -f infapp/module/*.py
rm -r infapp/__pycache__/ -f
rm -r infapp/module/__pycache__/ -f

echo "... check application version ..."
python3 -m infapp -v 

echo "... config AMQP variables ..."
python3 -m infapp conf --amqp-endpoint ${AMQP_ENDPOINT} --amqp-instanceid ${AMQP_INSTANCEID} --amqp-port ${AMQP_PORT} --amqp-virtualhost ${AMQP_VIRTUALHOST}

echo "... config MQTT variables ..."
python3 -m infapp conf --mqtt-endpoint ${MQTT_ENDPOINT} --mqtt-groupid ${MQTT_GROUPID} --mqtt-instanceid ${MQTT_INSTANCEID}

echo "... config Bucket and Database variables ..."
python3 -m infapp conf --bucket-url ${BUCKET_URL} --db-nodes ${DB_NODES} --db-port ${DB_PORT} --db-replicaset ${DB_REPLICASET}

echo "... run service ${SERVICE_NAME} ..."
python3 /opt/innoflex/infapp/${SERVICE_NAME}.pyc
