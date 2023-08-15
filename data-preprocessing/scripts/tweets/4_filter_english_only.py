import json
import os
from google.cloud import storage

# Create a client for Google Cloud Storage
client = storage.Client()

# Define the names of the input and output files and buckets
input_bucket_name = "twibot-22-parsed"
output_bucket_name = "twibot-22-parsed-english-only"
output_file_for_authors_list = "reduced_tweets_authors.txt"
filename_base = "tweets_"
json_extension = ".json"
num_of_files = 106

reduced_tweets_authors = set()

# Iterate over all diveded data files:
for indx in range(num_of_files):
    input_file = filename_base + str(indx).zfill(3) + json_extension
    output_file = input_file

    print("Input file: ", input_file)
    input_bucket = client.bucket(input_bucket_name)
    input_blob = input_bucket.blob(input_file)
    org_input_data = input_blob.download_as_string().decode("utf-8")

    input_data = []
    for line in org_input_data.splitlines():
        input_data.append(json.loads(line))

    for item in input_data:
            with open(output_file, "a+") as f:
                    if (('lang' in item) and item['lang']):
                        if ('en' == item['lang']):
                            f.write(json.dumps(item) + "\n")
                        else:
                            reduced_tweets_authors.add(item['author_id'])

    print("Saving filter data to bucket..")

    reduced_tweets_authors = set(reduced_tweets_authors)
    with open(output_file_for_authors_list, "a+") as file:
        for item in list((reduced_tweets_authors)):
            file.write(str(item)+"\n")

    # # Store the output file in Google Cloud Storage
    output_bucket = client.bucket(output_bucket_name)
    output_blob = output_bucket.blob(output_file)
    output_blob.upload_from_filename(output_file)

    path = "./" + output_file
    os.remove(path)

# Store file with reduced tweets author ids set in Google Cloud Storage
output_bucket = client.bucket(output_bucket_name)
output_blob = output_bucket.blob(output_file_for_authors_list)
output_blob.upload_from_filename(output_file_for_authors_list)