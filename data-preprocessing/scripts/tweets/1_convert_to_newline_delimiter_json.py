import json
from google.cloud import storage

# Create a client for Google Cloud Storage
client = storage.Client()

# Define the names of the input and output files
input_bucket_name = "twibot-22-bucket"
input_file = "tweet_8.json"
output_bucket_name = "twibot-22-converted-bucket"
output_file = "tweet_nd_8.json"

# Read the input file from Google Cloud Storage
input_bucket = client.bucket(input_bucket_name)
input_blob = input_bucket.blob(input_file)
input_data = input_blob.download_as_string().decode("utf-8")

# Parse the single-line JSON string and convert it to a list of dictionaries
input_data = json.loads("[" + input_data + "]")

# Write the list of dictionaries to a newline-delimited JSON format
with open(output_file, "w") as f:
    for item in input_data:
        f.write(json.dumps(item) + "\n")

# Store the output file in Google Cloud Storage
output_bucket = client.bucket(output_bucket_name)
output_blob = output_bucket.blob(output_file)
output_blob.upload_from_filename(output_file)