import requests
import json
from datetime import datetime

#from kafka import KafkaProducer

import boto3

access_key_id = 'AKIAYUACYWBUNDDYX3PW'
secret_access_key = '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'

session = boto3.Session(
    aws_access_key_id=access_key_id,
    aws_secret_access_key=secret_access_key
)

s3_client = session.client('s3')
s3_resource = session.resource('s3')

bucket = 'spark22-kafka-streaming'

#s3_client.list_objects(
#    Bucket = bucket
#)['Contents']

s3_bucket = s3_resource.Bucket(bucket)
s3_bucket_folder_name = "twitter-streaming/"

for obj in s3_bucket.objects.all().filter(Prefix=s3_bucket_folder_name + "checkpoint"):
    last_checkpoint_file_name = obj.key
    last_checkpoint_id = last_checkpoint_file_name.replace(s3_bucket_folder_name, "").replace('checkpoint-', '').replace('.chk', '')

#s3_objects = s3_client.list_objects(
#    Bucket='spark22-20201228'
#)

#topic_name = 'tweets'
#kafka_server = 'localhost:9092'

BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAJbpIwEAAAAAk4Ed%2Bi6Bb11C5%2FhFXADeC24cPNg%3DZYcCYf3WfukS1YSnOS8VEzHBpqj2zVDmYhoZLO3vnhgnaAM0Uo"

#current_utc_datetime = datetime.utcnow()
#current_utc_datetime_rfc_3339 = current_utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
#from_datetime = '2021-05-14T00:00:00Z'
#to_datetime = current_utc_datetime_rfc_3339

def search_twitter(query, last_newest_id, number_of_results, tweet_fields, bearer_token = BEARER_TOKEN):
    #start_time = "start_time={}".format(from_datetime)
    #end_time = "end_time={}".format(to_datetime)
    max_results = "max_results={}".format(number_of_results)
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
last_newest_id = "1393573728856989698"
number_of_results = 10

json_response = search_twitter(
    query=query, 
    last_newest_id=last_checkpoint_id, 
    number_of_results=number_of_results, 
    tweet_fields=tweet_fields, 
    bearer_token=BEARER_TOKEN
)

def flatten_tweet_json_response(json_response):
    tweet_list = []
    for tweet in json_response["data"]:
        tweet_flatten = {
            'id': tweet['id']
            , 'created_at': tweet['created_at']
            , 'author_id': tweet['author_id']
            , 'text': tweet['text']
            , 'retweet_count': tweet['public_metrics']['retweet_count']
            , 'reply_count': tweet['public_metrics']['reply_count']
            , 'like_count': tweet['public_metrics']['like_count']
            , 'quote_count': tweet['public_metrics']['quote_count']
        }
        tweet_list.append(tweet_flatten)
    return tweet_list

def users_json_response(json_response):
    user_list = []
    user_list = json_response["includes"]["users"]
    return user_list

tweet_list = flatten_tweet_json_response(json_response)
user_list = users_json_response(json_response)

tweet_list_formatted = json.dumps(tweet_list, indent=4, sort_keys = True)
user_list_formatted = json.dumps(user_list, indent=4, sort_keys = True)

current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
twitter_list_json_filename = '{}twitter_list_{}.json'.format(s3_bucket_folder_name,current_datetime)
user_list_json_filename = '{}user_list_{}.json'.format(s3_bucket_folder_name,current_datetime)
metadata_json_filename = '{}metadata_{}.json'.format(s3_bucket_folder_name,current_datetime)

###Twitter list json file
s3_object = s3_resource.Object(bucket, twitter_list_json_filename)
s3_object.put(
    Body=bytes(tweet_list_formatted.encode('UTF-8'))
)

###User list json file
s3_object = s3_resource.Object(bucket, user_list_json_filename)
s3_object.put(
    Body=bytes(user_list_formatted.encode('UTF-8'))
)

###Metadata json file
s3_object = s3_resource.Object(bucket, metadata_json_filename)
s3_object.put(
    Body=bytes(json.dumps(json_response['meta'], indent=4, sort_keys=True).encode('UTF-8'))
)

new_checkpoint_id = json_response["meta"]["newest_id"]

new_checkpoint_file_name = '{}checkpoint-{}.chk'.format(s3_bucket_folder_name,new_checkpoint_id)

s3_object = s3_resource.Object(bucket, last_checkpoint_file_name)
s3_object.delete()
s3_object = s3_resource.Object(bucket, new_checkpoint_file_name)
s3_object.put()