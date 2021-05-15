import requests
import json
from datetime import datetime

from kafka import KafkaProducer

topic_name = 'tweets'
kafka_server = 'localhost:9092'

BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAJbpIwEAAAAAk4Ed%2Bi6Bb11C5%2FhFXADeC24cPNg%3DZYcCYf3WfukS1YSnOS8VEzHBpqj2zVDmYhoZLO3vnhgnaAM0Uo"

current_utc_datetime = datetime.utcnow()
current_utc_datetime_rfc_3339 = current_utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
from_datetime = '2021-05-14T00:00:00Z'
to_datetime = current_utc_datetime_rfc_3339
last_newest_id = "1393573728856989698"

def search_twitter(query, tweet_fields, bearer_token = BEARER_TOKEN):
    start_time = "start_time={}".format(from_datetime)
    end_time = "end_time={}".format(to_datetime)
    max_results = "max_results=10"
    since_id = "since_id={}".format(last_newest_id)
    
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    #url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}&{}".format(
    #    query, tweet_fields,start_time,end_time,max_results
    #)

    url = "https://api.twitter.com/2/tweets/search/recent?query={}&{}&{}&{}".format(
        query,tweet_fields,max_results,since_id
    )

    response = requests.request("GET", url, headers=headers)
    print(response.status_code)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

query = "doge"
#tweet_fields = "tweet.fields=text,author_id,created_at"
tweet_fields = "tweet.fields=text,author_id,created_at,public_metrics&expansions=author_id&user.fields=description"

json_response = search_twitter(query=query, tweet_fields=tweet_fields, bearer_token=BEARER_TOKEN)

#json_response["meta"]["oldest_id"]
#json_response["meta"]["newest_id"]

print(json.dumps(json_response, indent=4, sort_keys=True))

import boto3

access_key_id = 'AKIAYUACYWBUNDDYX3PW'
secret_access_key = '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'

session = boto3.Session(
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key
)

s3_client = session.client('s3')

#s3_objects = s3_client.list_objects(
#    Bucket='spark22-20201228'
#)


bucket = 'spark22-kafka-streaming'
new_checkpoint_file = open("checkpoint-1393577556570411016.chk", "x")
new_checkpoint_filename = new_checkpoint_file.name
s3_bucket_folder_name = "twitter-streaming/"
object_name = s3_bucket_folder_name + new_checkpoint_filename
#print(object_name)
s3_client.upload_file(new_checkpoint_filename, bucket, object_name)

s3_client.delete_object(
    Bucket=bucket,
    Key="twitter-streaming/1393577556570411016.checkpoint"
)


for bucket in s3_client.list_buckets()['Buckets']:
    print(bucket['Name'])

for obj in s3_client.list_objects(Bucket='spark22-kafka-streaming', Prefix=s3_bucket_folder_name + "checkpoint")["Contents"]:
    print(obj["Key"])

import os
os.path.exists(new_checkpoint_filename)
os.remove(new_checkpoint_filename)

s3_resource = session.resource('s3')
s3_bucket = s3_resource.Bucket(bucket)
for obj in s3_bucket.objects.all().filter(Prefix=s3_bucket_folder_name + "checkpointaa"):
    print(obj)

s3_client = l