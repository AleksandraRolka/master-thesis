import json
import os
from google.cloud import storage

# Create a client for Google Cloud Storage
client = storage.Client()

# Define the names of the input and output files and buckets
input_bucket_name = "twibot-22-devided"
output_bucket_name = "twibot-22-parsed"
filename_base = "tweets_"
json_extension = ".json"
num_of_files = 106


# Iterate over all files:
for indx in range(num_of_files):
  input_file = filename_base + str(indx).zfill(3) + json_extension
  output_file = input_file

  print("Reading input file: ", input_file)
  # Read the input file from Google Cloud Storage
  input_bucket = client.bucket(input_bucket_name)
  input_blob = input_bucket.blob(input_file)
  org_input_data = input_blob.download_as_string().decode("utf-8")

  input_data = []
  for line in org_input_data.splitlines():
      input_data.append(json.loads(line))

  print("Start parsing..")
  for item in input_data:
          with open(output_file, "a+") as f:
                  if (('geo' in item) and item['geo']):
                      if ('coordinates' in item['geo']):
                          if ('coordinates' in item['geo']['coordinates']):
                              item['geo']['coordinates'] = item['geo']['coordinates']['coordinates']
                              if ('type' in item['geo']['coordinates']):
                                  item['geo']['type'] = item['geo']['coordinates']['type']
                          latitude, longitude = item['geo']['coordinates']
                          item['geo']['coordinates'] = {}
                          item['geo']['coordinates']['latitude'] = float(latitude)
                          item['geo']['coordinates']['longitude'] = float(longitude)
                  f.write(json.dumps(item) + "\n")

  print("Saving parsed data to bucket..")
  # Store the output file in Google Cloud Storage
  output_bucket = client.bucket(output_bucket_name)
  output_blob = output_bucket.blob(output_file)
  output_blob.upload_from_filename(output_file)

  path = "./" + output_file
  os.remove(path)