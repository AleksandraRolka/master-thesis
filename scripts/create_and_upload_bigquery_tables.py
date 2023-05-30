from google.cloud import bigquery

client = bigquery.Client()
project_id = "master-thesis-2023-384819"
dataset = "twibot_22_dataset"
location = "US"

path_to_schema = "./schema/tweets_schema.json"
job_config = bigquery.LoadJobConfig(
    schema=client.schema_from_json(path_to_schema),
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

filename_base = "tweets_"
json_extension = ".json"
num_of_files = 106


for indx in range(num_of_files):
  filename = "tweets_" + str(indx).zfill(3)
  table_name = filename                                       # Destination BigQuery table name
  json_file = filename + json_extension                       # Source dataset (new delimiter) json file
  table_id = project_id + "." + dataset + "." + table_name    # ID of the table to create
  uri = "gs://twibot-22-parsed/" + json_file                  # Gsutil URI of dataset json file (blob)
  
  print("Json file = ", json_file)

  load_job = client.load_table_from_uri(
      source_uris=uri,
      destination=table_id,
      location=location,
      job_config=job_config,
  )

  # Wait for the job to complete
  load_job.result()

  destination_table = client.get_table(table_id)
  print("Loaded {} rows.".format(destination_table.num_rows))