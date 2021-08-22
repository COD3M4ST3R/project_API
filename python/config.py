
# This file reads data from config.yml and assign for use

import yaml

# CONFIG FILE
with open('yaml/config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

# MINIO parameters
minio = config['minIO']  # Getting config data for minIO

minio_server = minio['server']
minio_userName = minio['userName']
minio_userPassword = minio['userPassword']



# RABBITMQ parameters
rabbitmq = config['rabbitMQ']

rabbitmq_server = rabbitmq['server']
rabbitmq_port = rabbitmq['port']
rabbitmq_userName = rabbitmq['userName']
rabbitmq_userPassword = rabbitmq['userPassword']
rabbitmq_virtualHost = rabbitmq['virtualHost']
