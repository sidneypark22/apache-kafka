# apache-kafka
Progress so far: kafka-twitter-streaming - Twitter streaming - python
1) This runs a twitter web api and get response from the request.
2) For data that is received from the request, it gets sent over via Kafka producer.
3) Once it is received by the Kafka consumer, data gets transformed into a flatten json format into 3 categories, twitter data, user data and api request response metadata.
4) Each data is uploaded to AWS s3 bucket as a json file.
5) At the end of the process, it will grab the latest twitter id from the request and upload it as a checkpoint file.
6) The checkpoint file with the latest twitter id retrieved is used as a watermark in the next twitter api request.

7) Next step is to build a data transformation process on either Databricks or EMR using PySpark to process files created by Kafka.
