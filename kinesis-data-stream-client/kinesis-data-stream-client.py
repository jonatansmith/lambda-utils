import json
import urllib.parse
import boto3
import base64
import os
import re
from json import JSONDecoder, JSONDecodeError
import uuid 
from uuid import uuid4
import sys


#Regular expression Constant to helps read Json
NOT_WHITESPACE = re.compile(r'[^\s]')

def decode_stacked(document, pos=0, decoder=JSONDecoder()):
    while True:
        match = NOT_WHITESPACE.search(document, pos)
        if not match:
            return
        pos = match.start()

        try:
            obj, pos = decoder.raw_decode(document, pos)
        except JSONDecodeError:
            # do something sensible if there s some error
            raise
        yield obj

def sendRecordsToKinesis(records, stream_name):
    print('SendRecordsToKinesis Method started')
    try:
        kinesis_rsp=kinesis.put_records(Records=records, StreamName=stream_name)
    except:
        s3resource = boto3.resource('s3')
        encoded_string = ''
        for record in records:
            encoded_string += str(json.dumps(record['Data']).encode("utf-8"))
        bucket_name = os.environ['DLQ_BUCKET']
        folder = os.environ['DLQ_FOLDER']
        key = str(uuid.uuid4())
        s3resource.Bucket(bucket_name).put_object(Key=folder+"/"+key, Body=encoded_string)
        print("Failed records sent to DQLBucket")
        raise
    if (kinesis_rsp['FailedRecordCount'] != 0):
        print("Failed to send to kinesis ", kinesis_rsp['FailedRecordCount'], "objects")
        retry_records = []
        for retryAtPosition, element in enumerate(kinesis_rsp['Records']):
            if (element.get('ErrorCode') or {}) == 'ProvisionedThroughputExceededException':
                #print('Failed to send to Kinesis due to ProvisionedThroughputExceededException. Retrying...')
                new_partition_key = str(uuid.uuid4())
                new_explicit_hash_key = str(uuid.uuid4().int)
                retry_record = {'Data': json.dumps(json.loads(records[retryAtPosition]['Data'])),'PartitionKey': new_partition_key, 'ExplicitHashKey': new_explicit_hash_key}
                retry_records.append(retry_record)
        print('Retrying send failed objects')
        records = retry_records
        retry_records=[]
        sendRecordsToKinesis(records,stream_name) #Recursive call to send failed records
        
    print('Data sent to kinesis sucessfully')


print('Loading function')
#starting clients
s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')
stream_name = os.environ['KINESIS_DATASTREAM']

def handler(event, context):
    #print(event) 
    #
    #print("Received event: " + json.dumps(event, indent=2))
    # Get the object from the event and show its content type
    #changed from event['Records'][0]['s3']['bucket']['name'] to get event in sns

    #SQS
    try:
        event_body = event['Records'][0]['body']
        sqsbody = json.loads(event_body)['Records'][0]
    except Exception as E:
        print('Event not in correct format: Key not found: ',E)
        sys.exit(1)

    try:
        bucket=sqsbody['s3']['bucket']['name']
        key=sqsbody['s3']['object']['key']

    except Exception as E:
        print('bucket/key not found in Event :',E)
        sys.exit(0)


    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        objectContent = response['Body'].iter_lines(chunk_size=1048576)
        for line in objectContent:
            i=0
            records=[]
            for obj in decode_stacked(str(line, encoding= 'utf-8')):
                i=i+1

                if (i == 500) :
                    print("Unloading to Kinesis first 500 elements before continue...")
                    sendRecordsToKinesis(records, stream_name)

                    i=0
                    records=[]
                #get uniq names for partition
                partition_key = str(uuid.uuid4())
                explicit_hash_key = str(uuid.uuid4().int)
                record = {'Data': json.dumps(obj),'PartitionKey': partition_key, 'ExplicitHashKey': explicit_hash_key}
                records.append(record)
            print("Sending ",i, " elements to kinesis...")
            sendRecordsToKinesis(records, stream_name)

            records=[]
        return 'Function Finished successfully'
    except Exception as e:
        print(e)
        print('Error running this lambda function. ')
        raise e


if __name__ == '__main__':
    f = open("Events/test-event-sqs-trigger.json", "r")#file name of trigger event in json
    context = {"key":"value"}
    event = json.loads(f.read())
    #print(event)
    handler(event, context)