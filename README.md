# apache-kafka
Personal project for apache-kafka
Progress so far:
Twitter streaming - python
This runs a twitter web api and get response from the request.
For data that is received from the request, it gets sent over via Kafka producer.
Once it is received by the Kafka consumer, data gets transformed into a flatten json format into 3 categories, twitter data, user data and api request response metadata.
Each data is uploaded to AWS s3 bucket as a json file.
At the end of the process, it will grab the latest twitter id from the request and upload it as a checkpoint file.
The checkpoint file with the latest twitter id retrieved is used as a watermark in the next twitter api request.
