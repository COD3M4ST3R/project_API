from django.shortcuts import render
from minio import Minio
import yaml
import json


# Read config.yaml file
with open('management/config.yaml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

# MINIO parameters
minio_param = config['minio']
minio_server = minio_param['server']
minio_username = minio_param['username']
minio_password = minio_param['password']

class Task:
    def __init__(self, bucket, path, startDate, endDate, functionName, status):
        self.bucket = bucket
        self.path = path
        self.startDate = startDate
        self.endDate = endDate
        self.functionName = functionName
        self.status = status

class Data:
    def __init__(self, bucket, path, size, input, output):
        self.bucket = bucket
        self.path = path
        self.size = size
        self.input = input
        self.output = output

list_task = list()
list_data = list()

# Create your views here.
def management(request):

    client = Minio(minio_server, access_key=minio_username, secret_key=minio_password, secure=False)

    # Display buckets
    buckets = client.list_buckets()
    for bucket in buckets:

        # Display tasks
        tasks = client.list_objects(bucket.name, 'task', recursive=True)
        for task in tasks:
            received_task = json.loads(client.get_object(bucket.name, task.object_name).read())

            if type(received_task) == dict:
                if received_task['status'] == 'pending':
                    object_task = Task(bucket=bucket.name, path=task.object_name, startDate=received_task['startDate'], endDate=' ', functionName=' ', status=received_task['status'])
                    list_task.append(object_task)
                if received_task['status'] == 'started':
                    object_task = Task(bucket=bucket.name, path=task.object_name, startDate=received_task['startDate'], endDate=' ', functionName=' ', status=received_task['status'])
                    list_task.append(object_task)
                else:#received_task['status'] = 'failed' or received_task['status'] == 'completed':
                    object_task = Task(bucket=bucket.name, path=task.object_name, startDate=received_task['startDate'], endDate=received_task['endDate'], functionName=received_task['functionName'], status=received_task['status'])
                    list_task.append(object_task)

            if type(received_task) == str:
                received_task = json.loads(received_task)
                if received_task['status'] == 'pending':
                    object_task = Task(bucket=bucket.name, path=task.object_name, startDate=received_task['startDate'], endDate=' ', functionName=' ', status=received_task['status'])
                    list_task.append(object_task)


        # Display datas
        datas = client.list_objects(bucket.name, 'data', recursive=True)
        for data in datas:
            received_data = client.get_object(bucket.name, data.object_name).read()
            received_data = json.loads(received_data)

            object_data = Data(bucket=bucket.name, path=data.object_name, size=data.size, input=received_data['input'], output=received_data['output'])
            list_data.append(object_data)




    return render(request, 'management.html', {'list_task': list_task, 'list_data': list_data})
