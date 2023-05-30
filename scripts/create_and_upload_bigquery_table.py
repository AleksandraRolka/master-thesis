from google.cloud import bigquery, schema_from_json

# BigQuery client object
client = bigquery.Client()
# Destination dataset location
location = "US"
project = "master-thesis-2023"
dataset = "twibot_22_dataset"
source_json_file = "tweets_" + str(0).zfill(3)
table_name = source_json_file[:-len(".json")]
# ID of the table to create
table_id = '.'.join(project, dataset, table_name)
# Gsutil URI of dataset json file (blob)
uri = "gs://twibot-22-parsed/" + source_json_file

path_to_schema_json_file = "./schema/tweets_schema.json"

job_config = bigquery.LoadJobConfig(
    schema=schema_from_json(path_to_schema_json_file),
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

load_job = client.load_table_from_uri(
    source_uris=uri,
    destination=table_id,
    location=location,
    job_config=job_config,
)

# Waits for the job to complete
load_job.result()  

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))