from google.cloud import bigquery
from gcp_env import PROJECT_ID, LOCATION


# Create a client for Big Query service
client = bigquery.Client()
project_id = PROJECT_ID
location = LOCATION
dataset = "twitbot_22_preprocessed"     # Destination BigQuery existing dataset name
table_name = "users"                    # Destination BigQuery table name
path_to_schema = "./../../schema/users_schema_preprocessed_reduced.json"

job_config = bigquery.LoadJobConfig(
    schema=client.schema_from_json(path_to_schema),
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

json_file = "users.json"                                    # Source dataset (new delimiter) json file
table_id = PROJECT_ID + "." + dataset + "." + table_name    # ID of the table to create
uri = "gs://twibot-22-bucket-preprocessed/" + json_file     # Gsutil URI of dataset json file (blob)

print("Json file with data to be uploaded: ", json_file)

load_job = client.load_table_from_uri(
    source_uris=uri,
    destination=table_id,
    location=location,
    job_config=job_config,
)

# Wait for the job to complete
load_job.result()

destination_table = client.get_table(table_id)
print("Currently {} rows are loaded to the table.".format(destination_table.num_rows))
print("Data from all files loaded to BigQueary table.")