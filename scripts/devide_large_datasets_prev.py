# The script is used to devide large size json files 
# and parsed them from single-line JSON string 
# to new line delimiter json.

import json
from google.cloud import storage


# Create a client for Google Cloud Storage
client = storage.Client()

print("Beggining..") # ----------------------------------------------------------------- # to delete

# Define names of the input and output files and Google Cloud Storage buckets
input_bucket_name = "twibot-22-orginal-dataset-bucket"
output_bucket_name = "twibot-22-devided"
input_files_list = ["tweet_0.json", "tweet_1.json"]
output_file_base = "tweets_"
json_extension = ".json"

print("Inputs..") # -------------------------------------------------------------------- # to delete


print("Start parsing..") # ----------------------------------------------------------------- # to delete

# Parse the single-line JSON string and convert it to a list of dictionaries
# Write the list of dictionaries to a newline-delimited JSON format

max_num_of_elements = 800000   # max number of elements in single output file
main_indx = 0                   # to create continuous new file numbering

for input_file in input_files_list:
    # Read the input file from Google Cloud Storage
    print("Before inner for..") # ----------------------------------------------------------------- # to delete
    input_bucket = client.bucket(input_bucket_name)
    input_blob = input_bucket.blob(input_file)
    input_data = input_blob.download_as_string().decode("utf-8")
    input_data = json.loads(input_data)
    output_files = set()
    output_file = output_file_base + str(main_indx) + json_extension
    output_files.add(output_file)

    print("Before inner for..") # ----------------------------------------------------------------- # to delete
    i = 0 # for indexing json elements
    for item in input_data:
            i += 1
            if i > max_num_of_elements:
                    i = 0
                    main_indx += 1
                    print("main_indx = ", main_indx)
                    output_file = output_file_base + str(main_indx) + json_extension
                    output_files.add(output_file)

            with open(output_file, "a+") as f:
                    f.write(json.dumps(item) + "\n")


    print("For done.. saving files") # ----------------------------------------------------------------- # to delete
    # Store the output file in Google Cloud Storage
    for out_file in output_files:
            output_bucket = client.bucket(output_bucket_name)
            output_blob = output_bucket.blob(out_file)
            output_blob.upload_from_filename(out_file)