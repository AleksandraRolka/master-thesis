import json
import os
import traceback
from google.cloud import storage
from common_preprocessing_utils import convert_to_unix_time


# Create a client for Google Cloud Storage
client = storage.Client()

input_bucket_name1 = "twibot-22-bucket-orginal-dataset"
input_bucket_name2 = "twibot-22-bucket-parsed-english-only"
output_bucket_name = "twibot-22-bucket-preprocessed"
label_file = "label.json"
input_users_file = "users.json"
output_file = input_users_file

# Read users' ids and labels (human/bot) to dict (without prefix "u" if exist)
input_bucket = client.bucket(input_bucket_name1)
input_blob = input_bucket.blob(label_file)
org_input_data = input_blob.download_as_string().decode("utf-8")

labels_dict = {}
for line in org_input_data.splitlines():
    item = json.loads(line)
    id = item['id'][1:] if item['id'].startswith("u") else item['id']
    label = item['label']
    labels_dict[str(id)] = item['label']


# Read users' data
input_bucket = client.bucket(input_bucket_name2)
input_blob = input_bucket.blob(input_users_file)
org_input_data = input_blob.download_as_string().decode("utf-8")

input_data = []
for line in org_input_data.splitlines():
    input_data.append(json.loads(line))


for item in input_data:
    with open(output_file, "a+") as f:
        try:
            # Clear new structure's fields 
            transformed = {}
            transformed['id'] = None
            transformed['label'] = None
            transformed['username'] = ""
            transformed['name'] = ""
            transformed['created_at'] = ""
            transformed['verified'] = False
            transformed['protected'] = False
            transformed['withheld'] = False 
            transformed['has_location'] = False
            transformed['location'] = None
            transformed['has_profile_image_url'] = False
            transformed['has_pinned_tweet'] = False
            transformed['url'] = ""
            transformed['followers_count'] = 0
            transformed['following_count'] = 0
            transformed['tweet_count'] = 0
            transformed['listed_count'] = 0
            transformed['has_description'] = False
            transformed['description'] = ""
            transformed['descr_no_hashtags'] = 0
            transformed['descr_no_cashtags'] = 0
            transformed['descr_no_mentions'] = 0
            transformed['descr_no_urls'] = 0
            transformed['url_no_urls'] = 0

            # column withheld: { country_codes }
            # simplified info: boolean column: withheld
            if ('withheld' in item and item['withheld']):
                transformed['withheld'] = True 
            # column username: -
            if ('username' in item):
                transformed['username'] = item['username']
            # column url: -
            if ('url' in item):
                transformed['url'] = item['url']
            # column public_metrics: listed_count, tweet_count, following_count, followers_count : flatten structure, remove top level field
            if ('public_metrics' in item):
                transformed['listed_count'] = item['public_metrics']['listed_count']
                transformed['tweet_count'] = item['public_metrics']['tweet_count']
                transformed['following_count'] = item['public_metrics']['following_count']
                transformed['followers_count'] = item['public_metrics']['followers_count']
            # column description: add boolen column
            if ('description' in item and item['description']):
                transformed['description'] = item['description']
                transformed['has_description'] = True
            # column protected: -
            if ('protected' in item and item['protected']):
                transformed['protected'] = item['protected']
            # column profile_image_url: 
            if ('profile_image_url' in item and item['profile_image_url']):
                transformed['has_profile_image_url'] = True
            # column pinned_tweet_id: 
            if ('pinned_tweet_id' in item and item['pinned_tweet_id']):
                transformed['has_pinned_tweet'] = True
            # column name: -
            if ('name' in item):
                transformed['name'] = item['name'] 
            # column location: 
            if ('location' in item and item['location']):
                transformed['location'] = item['location'] 
                transformed['has_location'] = True
            # column id: remove prefix "u" if exists
            if ('id' in item):
                transformed['id'] = item['id'][1:] if item['id'].startswith("u") else item['id']
            # column entities:
            # {
            #   description: {
            #       cashtags: { tag, start, end}, 
            #       mentions: { username, start, end }, 
            #       hashtags: { tag, start, end }, 
            #       urls: { display_url, expanded_url, end, url, start },
            #   },
            #   url: { 
            #       urls: { display_url, expanded_url, end, url, start } 
            #   } 
            # }
            if (item['entities']):
                if ('description' in item['entities'] and item['entities']['description']):
                    if ('cashtags' in item['entities']['description'] and item['entities']['description']['cashtags']):
                            transformed['descr_no_cashtags'] = len(item['entities']['description']['cashtags'])
                    if ('mentions' in item['entities']['description'] and item['entities']['description']['mentions']):
                            transformed['descr_no_mentions'] = len(item['entities']['description']['mentions'])
                    if ('hashtags' in item['entities']['description'] and item['entities']['description']['hashtags']):
                            transformed['descr_no_hashtags'] = len(item['entities']['description']['hashtags'])
                    if ('urls' in item['entities']['description'] and item['entities']['description']['urls']):
                            transformed['descr_no_urls'] = len(item['entities']['description']['urls'])
                if ('url' in item['entities'] and item['entities']['url']):
                    if ('urls' in item['entities']['url'] and item['entities']['url']['urls']):
                            transformed['url_no_urls'] = len(item['entities']['url']['urls'])
            # column verified: 
            if ('verified' in item and item['verified']):
                transformed['verified'] = True
            # column created_at:
            if ('created_at' in item):
                datetime_string = item['created_at']
                transformed['created_at'] = convert_to_unix_time(datetime_string)
            # label column added based od label.csv / label.json data
            user_id = transformed['id']
            if user_id in labels_dict.keys():
                transformed['label'] = labels_dict[user_id]
            else:
                 print("Can not find label for user id: ", user_id)
            
            f.write(json.dumps(transformed) + "\n")

        except Exception as e: 
            print("Tweet before:")
            print(item)
            print("Tweet after:")
            print(transformed)
            print("Error message:")
            print(traceback.print_exc())

print("Saving preprocessed data to bucket..")

# Store the output file in Google Cloud Storage
output_bucket = client.bucket(output_bucket_name)
output_blob = output_bucket.blob(output_file)
output_blob.upload_from_filename(output_file)

path = "./" + output_file
os.remove(path)