from google.cloud import bigquery
from gcp_env import PROJECT_ID, LOCATION

import pandas as pd

import pycountry
import geograpy
import nltk
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('maxent_ne_chunker')
nltk.download('words')


def map_to_country(record):
    org_location = record['location']
    if (org_location != "" or not (org_location is None)):
        try:
            countries = geograpy.get_place_context(text=org_location).countries
            if (len(countries) > 0):
                return countries[0]
            else:
                delimiter_regex = r',|, |/|\\|..|–|\|'
                location_splits = org_location.split(delimiter_regex)
                found_location = ''
                for location in location_splits:
                    countries = geograpy.get_place_context(text=location).countries
                    if (len(countries) > 0):
                        found_location = countries[0]
                        break
                if (found_location != ''):
                    return found_location
                else:
                    return 'Unknown'
        except Exception as e:
            # print("Error")
            # traceback.print_exc()
            # print(org_location)
            # print():
            return 'Unknown'
    else:
        return'None'

# Dict and names list of all countries used by supporting lib
all_country_list = pycountry.countries
len(all_country_list)
all_country_dict = {'None' : 0,
                    'Unknown' : 1}
i = 2
for country in all_country_list:
    all_country_dict[country.name] = i
    i += 1

all_country_names = list(all_country_dict.keys())


def map_country_name_to_num_class(country_name, all_countries_names=all_country_names, all_countries_dict=all_country_dict):
    if (country_name in all_countries_names):
        return all_countries_dict[country_name]
    elif (country_name == ""):
        return all_countries_dict['None']
    else:
        print("Unknown: ", country_name)
        return all_countries_dict['Unknown']

# --------------------------------------------------------------------------------------------------------

# Create a client for Big Query service
bqclient = bigquery.Client()
project_id = PROJECT_ID
location = LOCATION

dataset = "twitbot_22_preprocessed_common_users_ids" # BigQuery existing dataset name

source_table_name = "users" # Source BigQuery table name
BQ_SRC_TABLE_USERS = dataset + "." + source_table_name

# Load data from source table
print("Loading data from source table..")
SQL_QUERY = f"""SELECT * FROM {BQ_SRC_TABLE_USERS}"""
users_df = bqclient.query(SQL_QUERY).to_dataframe()


## Add column with country name and mapped numeric value in another column
print("Mapping location - country info..")

dest_table_name = "users_with_country_temp" # Destination BigQuery table name
BQ_DEST_TABLE_USERS = dataset + "." + dest_table_name
path_to_dest_schema = "./../../schema/users_schema_preprocessed_reduced_with_country.json"
# path_to_dest_schema = "./users_schema_preprocessed_reduced_with_country.json"

job_config = bigquery.LoadJobConfig(
        schema=bqclient.schema_from_json(path_to_dest_schema)
)
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

org_users_df = pd.DataFrame(users_df).copy()
max_rows = len(users_df)
start_indx = 0
max_num_rows_in_single_df = 200

while (start_indx < max_rows):
    new_df = pd.DataFrame(columns=(users_df.columns))
    print("Curr start_indx = ", start_indx)
    users_df = org_users_df[start_indx:]
    new_df = pd.DataFrame(columns=(users_df.columns))
    k = 0
    for index, record in users_df.iterrows():
        if (k >= max_num_rows_in_single_df):
            break
        country = map_to_country(record)
        if (country == 'United States of America'):
            country = 'United States'
        record['country'] = country
        record['country_numeric'] = map_country_name_to_num_class(record['country'])
        # new_df.append(record)
        new_df = pd.concat([new_df, record.to_frame().T])
        k += 1
    start_indx += k
    # Save new dataframe to Big Query Table
    load_job = bqclient.load_table_from_dataframe(
        pd.DataFrame(new_df).copy(), BQ_DEST_TABLE_USERS, job_config=job_config
    )
    # Wait for the job to complete
    load_job.result()
    print("Next batch saved")

print("All records saved in  destination Big Query table")
print("DONE")