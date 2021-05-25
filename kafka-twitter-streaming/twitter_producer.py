import requests
import boto3
from kafka import KafkaProducer, producer
import time
import json

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

def get_twitter_json_response(query,number_of_results,tweet_fields):
    for obj in s3_bucket.objects.all().filter(Prefix=s3_bucket_folder_name + "checkpoint"):
        last_checkpoint_file_name = obj.key
        last_checkpoint_id = last_checkpoint_file_name.replace(s3_bucket_folder_name, "").replace('checkpoint-', '').replace('.chk', '')
    json_response = search_twitter(
    query=query, 
    last_newest_id=last_checkpoint_id, 
    number_of_results=number_of_results, 
    tweet_fields=tweet_fields, 
    bearer_token=BEARER_TOKEN
    )
    return json_response

query = "doge"
#tweet_fields = "tweet.fields=text,author_id,created_at"
tweet_fields = "tweet.fields=text,author_id,created_at,public_metrics&expansions=author_id&user.fields=description"
#last_newest_id = "1393573728856989698"
number_of_results = 10

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

for i in range(5):
    json_response = get_twitter_json_response(query,number_of_results,tweet_fields)
    test_json_byte = bytes(json.dumps(json_response).encode('UTF-8'))
    producer.send('twitter-topic', test_json_byte)
    time.sleep(20)
    print('twitter json reponse sent {}'.format(i))