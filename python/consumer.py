
# Consumes queue from learning query, give key to reader.py and updates the status of task

from io import BytesIO
import pika
import json
from minio import Minio
from urllib3.exceptions import ResponseError
from config import minio_server, minio_userName, minio_userPassword, rabbitmq_server, rabbitmq_port, rabbitmq_userName, rabbitmq_userPassword, rabbitmq_virtualHost



                                                                     # FUNCTIONS

# Gets type of bytes as a parameter then convert is to JSON
def convertBytes(data_bytes):
    data_json = json.loads(data_bytes)
    return data_json




# Initialize to start task
def initializer(projectName, taskID, functionName, status):

    # MINIO
    # Minio credentials
    client = Minio(minio_server, access_key=minio_userName, secret_key=minio_userPassword, secure=False)
    path = 'task' + '/' + taskID

    try:
        # Get object
        data_bytes = json.loads(client.get_object(projectName, path).read())
        data_json = convertBytes(data_bytes=data_bytes)

        # Get minio data
        startDate = data_json['startDate']

        # Make payload ready for load
        payload_json = {"startDate": startDate, "functionName": functionName, "status": status}
        payload_string = json.dumps(payload_json)
        payload_bytes = BytesIO(bytes(payload_string, 'utf-8'))

        # Payload attributes
        size = payload_bytes.getbuffer().nbytes

        # Upload payload
        client.put_object(projectName, path, payload_bytes, size)

        # Look for function folder if there is function requested
        path_function = 'functions' + '/' + functionName

        try:
            requestedFunction = client.get_object(projectName, path_function).read()

            exec(requestedFunction, {"client": client, "functionName": functionName, "projectName": projectName, "taskID": taskID})


        except:
            print('%s is not exist!' % functionName)

    except ResponseError as err:
        print(err)




# Consumes queues from query called 'learning' with exchange called logs
def consume():

    # RABBITMQ: consumes data to created 'learning' query
    # RabbitMQ credentials
    global channel
    try:
        credentials = pika.PlainCredentials(rabbitmq_userName, rabbitmq_userPassword)
        parameters = pika.ConnectionParameters(rabbitmq_server, rabbitmq_port, rabbitmq_virtualHost, credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        ''' These tasks completed on UI to make query durable
        #channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)
        #channel.queue_declare(queue='learning', durable=True)
        #channel.queue_bind(exchange='logs', queue='learning')
        '''

    except:
        print('RABBITMQ Connection Failure')

    def callback(ch, method, properties, body):
        data = json.loads(body) # bytes converted to JSON
        initializer(projectName=data['projectName'], taskID=data['taskID'], functionName=data['functionName'], status='started') # send taskID and status:started to updateStatus

    channel.basic_consume(queue='learning', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()




consume()