
# This project has been written by Nadir Suhan ILTER for project named 'project_API'.

import json
import os
import django
import uuid
from datetime import datetime
from io import BytesIO
import pika
import flask
from config import minio_server, minio_userName, minio_userPassword, rabbitmq_server, rabbitmq_port, rabbitmq_userName, rabbitmq_userPassword, rabbitmq_virtualHost
from apispec import APISpec
from webargs import fields
from apispec.ext.marshmallow import MarshmallowPlugin
from flask import request, render_template, redirect, flash
from flask_apispec import FlaskApiSpec, marshal_with, use_kwargs
from marshmallow import Schema
from minio import Minio
from werkzeug.utils import secure_filename

                                                    # INITIALIZE

# MINIO CREDENTIALS
host = minio_server # Addres of MINIO Console location. Server located on KaliLinux
access_key = minio_userName # Default user account name
secret_key = minio_userPassword # Default user account password


# FLASK INITIALIZE
app = flask.Flask(__name__)


# SWAGGER MODEL INITIALIZING
class Model_UploadData(Schema): # This is model for uploading JSON data to MINIO server
    class Meta:
        fields = ('input', 'output')

class Model_Learning(Schema):
    class Meta:
        field = ('functionName') # This is a model created for API Learning

class Model_Status(Schema):
    class Meta:
        field = ('projectName') # This model created for API Status

class Model_UploadFile(Schema): # This is model for uploading data as JSON to MINIO server
    class Meta:
        file = fields.Raw(required=True)


# FILE SPECIFICATIONS
UPLOAD_FOLDER = 'C:/Users/nadir/Desktop/project_API'
ALLOWED_EXTENSIONS = {'zip'}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.secret_key = b'secret'




                                                    # FUNCTIONS

#FILE FORMAT CHECK --> This function checks whether chosen file by client is in the allowed extension or not
def allowedFiles(filename: str) -> bool:
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS




# DUPLICATE FILE CHECK --> This function checks file name inside the bucket to see if there is any duplicated name. If there
# is its return error otherwise uploads a file into bucket as requested file name
def duplicateFileCheck(client, bucketName, projectName):
    try:
        response = client.get_object(bucketName, projectName)
        return 1  # THERE ARE FILE DUPLICATE
    except:
        return 0  # FILE IS OKAY TO GO




                                                    # API START




### TEST
@app.route('/test', methods=['GET'])
def test():

    return django.get_version()
### TEST





# HOME
# This is where you first see when connect to API
@app.route('/')
def home():
    return render_template('home.html')




# MINIO DATA RECEIVER
# This API creates bucket from parameter getting from URI called <projectName> and upload JSON to filename called data

@app.route('/api/v1/feeding/data/<projectName>', methods=['GET', 'POST'])
@marshal_with(Model_UploadData(many=False), description='File has been uploaded successfully', code=201)
@marshal_with(Model_UploadData(many=False), description='Request part need to be a JSON', code=400)
@marshal_with(Model_UploadData(many=False), description='There are already a file uploaded. Change file name and try again', code=409)
@use_kwargs({'input': fields.Str(default='This is an input'), 'output': fields.Str(default='This is an output')})
def upload_data(projectName, input, output):

    json_data = flask.request.json  # JSON req is gained from flask
    string = json.dumps(json_data)

    # Data upload specifications for type string
    bucket = projectName # Bucket name created by URI parameter
    content = BytesIO(bytes(string, 'utf-8')) # String upload data converted from JSON
    id = str(uuid.uuid4())
    path = 'data' + '/' + id
    size = content.getbuffer().nbytes

    client = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
    found = client.bucket_exists(bucket)

    if not found:
        client.make_bucket(bucket)

    # Upload Data
    client.put_object(bucket, path, content, size)

    return "File has been successfully uploaded"




# MINIO MODEL RECEIVER
# This API creates bucket from parameter getting from URI called <projectName> and upload JSON to filename called model

@app.route('/api/v1/feeding/model/<projectName>', methods=['GET', 'POST'])
@marshal_with(Model_UploadData(many=False), description='File uploaded successfully', code=201)
@marshal_with(Model_UploadData(many=False), description='Request part need to be JSON', code=400)
@marshal_with(Model_UploadData(many=False), description='There are already a file uploaded. Change file name and try again', code=409)
@use_kwargs({'input': fields.Str(default='This is an input'), 'output': fields.Str(default='This is an output')})
def upload_model(projectName, input, output):

    json_data = flask.request.json  # JSON req is gained from flask
    string = json.dumps(json_data)

    # Data upload specifications for type string
    bucket = projectName
    content = BytesIO(bytes(string, 'utf-8'))
    id = str(uuid.uuid4())
    path = 'model' + '/' + id
    size = content.getbuffer().nbytes

    # Connection to MinIO host with credentials
    client = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
    found = client.bucket_exists(bucket)

    if not found:
        client.make_bucket(bucket)

    # Upload Data
    client.put_object(bucket, path, content, size)
    return "File has been successfully uploaded"




# API 1: FILE UPLOAD

@app.route('/api/v1/feeding/project_API', methods=['GET'])
@marshal_with(Model_UploadFile(many=True), code=201)
def api_1():
    return render_template('upload.html')

@app.route('/api/v1/feeding/project_API/uploader', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No File Part!')
            return redirect(request.url)

        file = request.files.get('file')

        if file.filename == '':
            flash('There are no file!')
            return redirect(request.url)

        if file and allowedFiles(file.filename):
            filename = secure_filename(file.filename)
            filename = str(uuid.uuid4()) + filename
            file.save(os.path.join(UPLOAD_FOLDER, filename))
            flash('File has been uploaded successfully')

            return render_template('home.html')



# API 2: STATUS
# This API gets taskID as a URI parameter and projectName as a JSON from request to search S3 to return status of task

@app.route('/api/v1/status/<taskID>', methods=['GET', 'POST'])
@marshal_with(Model_Status(many=False), description='Internal Server Error', code=400)
@use_kwargs({'projectName': fields.Str(default='this is a projectName')})
def status(taskID, projectName):

    # MINIO
    # Credentials
    client = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
    path = 'task' + '/' + taskID

    # Check for existence of projectName
    try:
        found = client.bucket_exists(projectName)
        if not found:
            return 'There are no any bucket called: %s' % (projectName)
    except:
        return 'Failure while trying to reach bucket: %s' % (projectName)

    # Check for existence of taskID
    try:
        data_json = client.get_object(projectName, path)
    except:
        return 'There are no any applied task or task with this taskID inside bucket: %s' % (projectName)

    data_json = json.loads(client.get_object(projectName, path).read())

    # Set minio data
    status = data_json['status']

    return status




# API 3: LEARNING
# This API creates a task file inside S3 then creates a queue inside learning query

@app.route('/api/v1/learning/<projectName>', methods=['GET', 'POST'])
@marshal_with(Model_Learning(many=False), description='Internal Server Error', code=400)
@use_kwargs({'functionName': fields.Str(default='this is a functionName')})
def learning(projectName, functionName):

    functionName_json = flask.request.json # Getting JSON request
    functionName_string = functionName_json['functionName'] # Converting request JSON to string

    currentTime = datetime.now()
    currentTime = currentTime.strftime("%d-%m-%Y-%H-%M-%S") # Gets current time
    generatedID = str(uuid.uuid4()) # Generate uuid4 id
    taskID = projectName + '_' + currentTime + '_' + generatedID # Complete taskID

    learning_json_dumps = json.dumps({'projectName': projectName, 'functionName': functionName_string, 'taskID': taskID}) # Completed data as JSON
    learning_json_loads = json.loads(learning_json_dumps)
    learning_bytes = json.dumps(learning_json_loads, indent=2).encode('utf-8')

    # MINIO: creates a JSON file inside projectName(bucket) / task / taskID and put JSON about status of task
    # status json
    status_json = json.dumps({'startDate': currentTime, 'status': 'pending'})
    status_string = json.dumps(status_json)

    # minio attributes
    bucket = projectName
    content = BytesIO(bytes(status_string, 'utf-8'))
    path = 'task' + '/' + taskID
    size = content.getbuffer().nbytes

    # minio credentials
    client = Minio(host, access_key=access_key, secret_key=secret_key, secure=False)
    found = client.bucket_exists(bucket) # look for bucket name

    if not found: # if bucket not already created, create a one
        client.make_bucket(bucket)

    # Upload Data
    client.put_object(bucket, path, content, size)


    # RABBITMQ: loads data to created 'learning' query
    # rabbitMQ credentials
    credentials = pika.PlainCredentials(rabbitmq_userName, rabbitmq_userPassword)
    parameters = pika.ConnectionParameters(rabbitmq_server, rabbitmq_port, rabbitmq_virtualHost, credentials)
    connection = pika.BlockingConnection(parameters)

    # Creating a channel
    channel = connection.channel()

    ''' These tasks completed on UI to make query durable
    channel.exchange_declare(exchange='logs', exchange_type='fanout', durable=True)
    channel.queue_declare(queue='learning', durable=True)
    channel.queue_bind(exchange='logs', queue='learning')
    '''
    channel.basic_publish(exchange='logs', routing_key='learning', body= learning_bytes)

    connection.close()
    return learning_json_loads # return the sent data to user in JSON format




# API 4: PREDICTION

@app.route('/api/v1/prediction/<analysID>', methods=['GET'])
def api_4(analysID):
    return "This is a future use created API for Prediction/analysID: %s" % (analysID)




# SWAGGER on /swagger-ui

app.config.update({
    'APISPEC_SPEC': APISpec(
        title='project_API',
        version='1.0.0',
        plugins=[MarshmallowPlugin()],
        openapi_version='2.0'
    ),
    'APISPEC_SWAGGER_URL': '/swagger/',
})
# Endpoints
docs = FlaskApiSpec(app)
docs.register(api_4)
docs.register(learning)
docs.register(status)
docs.register(upload_file)
docs.register(upload_data)
docs.register(upload_model)



### STARTING API

app.run()
