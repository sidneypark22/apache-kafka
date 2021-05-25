from kafka import KafkaConsumer

import json
from datetime import datetime
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

s3_bucket = s3_resource.Bucket(bucket)
s3_bucket_folder_name = "twitter-streaming/"

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

def upload_json_files_to_s3_bucket(tweet_list,user_list,json_response,last_checkpoint_file_name,current_datetime):
    #current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
    twitter_list_json_filename = '{}twitter_list_{}.json'.format(s3_bucket_folder_name,current_datetime)
    user_list_json_filename = '{}user_list_{}.json'.format(s3_bucket_folder_name,current_datetime)
    metadata_json_filename = '{}metadata_{}.json'.format(s3_bucket_folder_name,current_datetime)
    ###Twitter list json file
    s3_object = s3_resource.Object(bucket, twitter_list_json_filename)
    s3_object.put(
        Body=bytes(tweet_list.encode('UTF-8'))
    )
    ###User list json file
    s3_object = s3_resource.Object(bucket, user_list_json_filename)
    s3_object.put(
        Body=bytes(user_list.encode('UTF-8'))
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

consumer = KafkaConsumer(
    'twitter-topic',
    bootstrap_servers=['localhost:9092']
    )

for json_response_byte in consumer:
    print('step 1')
    json_response = json.loads(json_response_byte.value.decode('UTF-8'))
    print('step 2')
    tweet_list = flatten_tweet_json_response(json_response)
    print('step 3')
    user_list = users_json_response(json_response)
    print('step 4')
    tweet_list_formatted = json.dumps(tweet_list, indent=4, sort_keys = True)
    print('step 5')
    user_list_formatted = json.dumps(user_list, indent=4, sort_keys = True)    
    print('step 6')
    for obj in s3_bucket.objects.all().filter(Prefix=s3_bucket_folder_name + "checkpoint"):
        last_checkpoint_file_name = obj.key
        #last_checkpoint_id = last_checkpoint_file_name.replace(s3_bucket_folder_name, "").replace('checkpoint-', '').replace('.chk', '')
    print('step 7')
    current_datetime = datetime.now().strftime("%Y%m%d%H%M%S")
    upload_json_files_to_s3_bucket(tweet_list_formatted,user_list_formatted,json_response,last_checkpoint_file_name,current_datetime)
    print('files uploaded to s3 bucket with timestamp {}'.format(current_datetime))

