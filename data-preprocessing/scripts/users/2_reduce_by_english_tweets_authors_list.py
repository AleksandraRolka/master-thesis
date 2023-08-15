import json
import os
from google.cloud import storage


# Create a client for Google Cloud Storage
client = storage.Client()

# Define the names of the input and output files and buckets
input_bucket_name_1= "twibot-22-bucket-orginal-dataset"         # bucket where user.json file is stored
input_bucket_name_2 = "twibot-22-bucket-parsed-english-only"    # bucket where reduced_tweets_authors.txt file is stored
output_bucket_name = "twibot-22-bucket-parsed-english-only"
input_file_with_users_ids_to_remove = "reduced_tweets_authors.txt"
input_users_file = "user_nd.json"
output_file = "users.json"


# Read users ids to be removed from final dataset (those user has non english tweets)
input_bucket = client.bucket(input_bucket_name_2)
input_blob = input_bucket.blob(input_file_with_users_ids_to_remove)
authors_data = input_blob.download_as_string().decode("utf-8")

authors_to_reduced = set()
for id in authors_data.splitlines():
    authors_to_reduced.add(id)

# Read users data
input_bucket = client.bucket(input_bucket_name_1)
input_blob = input_bucket.blob(input_users_file)
org_input_data = input_blob.download_as_string().decode("utf-8")

input_data = []
for line in org_input_data.splitlines():
    input_data.append(json.loads(line))

for item in input_data:
    with open(output_file, "a+") as f:
        author_id = item['id'][1:] if item['id'].startswith("u") else item['id']
        item['id'] = author_id
        if (author_id not in authors_to_reduced):
            f.write(json.dumps(item) + "\n")
        
# Store the output file in Google Cloud Storage
output_bucket = client.bucket(output_bucket_name)
output_blob = output_bucket.blob(output_file)
output_blob.upload_from_filename(output_file)

path = "./" + output_file
os.remove(path)