
                                                                # DEFINITION

# Reads data from S3 and send order to update status respect to result


                                                                # IMPORTS
import time
from io import BytesIO
from datetime import datetime
from minio import Minio
import json



                                                                # VARIABLES

keys = ['This is a key list which contains all the keys(paths) of data models']
data_size = 0 # holds total size of data inside bucket
data_file = 0 # total number of elements is data folder
success = True # failed or completed decide variable
list_input = list() # holds data for inputs
list_output = list() # holds data for outputs


                                                                # FUNCTIONS

# Gets type of bytes as a parameter then convert is to JSON
def convertBytes(data_bytes):
    data_json = json.loads(data_bytes)
    return data_json




# Updates status by checking success variable
def updateStatus(projectName, taskID):

    # Updates status as completed
    if success is True:

        # MINIO
        # Attributes
        path = 'task' + '/' + taskID

        # Get object as string
        data = json.loads(client.get_object(projectName, path).read())

        # Set minio data
        startDate = data['startDate']
        functionName = data['functionName']

        # Make payload ready for load
        currentTime = datetime.now()
        currentTime = currentTime.strftime("%d-%m-%Y-%H-%M-%S")  # Gets current time
        payload_json = {"startDate": startDate, "endDate": currentTime,  "functionName": functionName, "status": 'completed'}
        payload_string = json.dumps(payload_json)
        payload_bytes = BytesIO(bytes(payload_string, 'utf-8'))

        # Payload attributes
        size = payload_bytes.getbuffer().nbytes

        # Upload payload
        client.put_object(projectName, path, payload_bytes, size)


    # Updates status as failed
    else:
        # MINIO
        # Attributes
        path = 'task' + '/' + taskID

        # Get object as string
        data = json.loads(client.get_object(projectName, path).read())

        # Set minio data
        startDate = data['startDate']
        functionName = data['functionName']

        # Make payload ready for load
        currentTime = datetime.now()
        currentTime = currentTime.strftime("%d-%m-%Y-%H-%M-%S")  # Gets current time
        payload_json = {"startDate": startDate, "endDate": currentTime, "functionName": functionName,
                        "status": 'failed'}
        payload_string = json.dumps(payload_json)
        payload_bytes = BytesIO(bytes(payload_string, 'utf-8'))

        # Payload attributes
        size = payload_bytes.getbuffer().nbytes

        # Upload payload
        client.put_object(projectName, path, payload_bytes, size)




# Counts inputs and outputs
def parseData():

    list_input.sort()
    list_output.sort()
    size_list = len(list_input)
    counter = 0

    print('\nOn list_input there are:\n')
    for index in range(0, size_list):

        if index != size_list - 1 and list_input[index + 1] is list_input[index]:
            counter = counter + 1

        else:
            print('There are %d of %s' % (counter, list_input[index]))
            counter = 1

    print('\nOn list_output there are:\n')
    for index in range(0, size_list):

        if index != size_list - 1 and list_output[index + 1] is list_output[index]:
            counter = counter + 1

        else:
            print('There are %d of %s' % (counter, list_output[index]))
            counter = 1




# Gets and process data from S3
def processData(path, projectName):
    global success

    try:
        data_json = json.loads(client.get_object(projectName, path).read())

        list_input.append(data_json['input'])
        list_output.append(data_json['output'])
    except:
        success = False




# Gets informatipon about data file
def receiveInformation(projectName):
    global data_size, data_file
    print('\nThis is reader.py\n')

    # Check for bucket
    try:
        client.bucket_exists(projectName)
    except:
        print('Bucket not exist!')

    try:
        objects = client.list_objects(projectName, 'data', recursive=True)

        for object in objects:
            keys.append(object.object_name) # append key to keys list
            data_size = data_size + object.size # calculate total size of data file
            data_file = data_file + 1 # calculate total number of elements in data folder

        print('Number of total data file inside bucket "%s": %d\nNumber of total size of data %d bytes' % (projectName, data_file, data_size))

    except:
        print('bucket: %s does not have any folder named "data" or does not have any data on it' % projectName)




# Gets bucket: 'projectName' and taskID to retrieve data from 'data' folder of bucket then gives keys to threads to read inside of data then prints it.
def receiveData(projectName, taskID):

    keys.pop(0) # deletes initialize description of list where will be only keys remains
    size = len(keys) # total size of paths of data's

    for index in range(0, size): # walking through all the keys and assign them to thread as args
        path = keys[index]
        processData(path=path, projectName=projectName)

    parseData()
    updateStatus(projectName=projectName, taskID=taskID) # updates status as 'completed' or 'failed' respect to success variable




# MAIN FLOW
print('\n This is function: reader.py\n')

time.sleep(10)
receiveInformation(projectName=projectName)
receiveData(projectName=projectName, taskID=taskID)
