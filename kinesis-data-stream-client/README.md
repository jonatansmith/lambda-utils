# kinesis-data-stream-client

## A Kinesis Data Stream client to read S3 object content and put data to Kinesis

# How it works
### A S3 trigger send object Key to SQS, which invokes lambda
### The execution reads content and try to send to Kinesis using all available shards, using random shard partitions
### Every failed attempt due to ProvisionedThroughputExceededException is retryed until succeed or timeout

# Search tags: Kinesis Client, Data Stream put records, ProvisionedThroughputExceededException

### How to use:
#### Create a Lambda python 3.8 
#### Configure Environment Variables:
* KINESIS_DATASTREAM - Kinesis Datastream name
* DLQ_BUCKET - Destination Bucket to save failed records
* DLQ_FOLDER - Prefix to save failed records


## TO DO
* Configure DLQ before timeout
* Set retry limit with expoonential backoff
