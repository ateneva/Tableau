import os
import json
from google.cloud import bigquery, storage

from glob import glob
from pathlib import Path
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NULLABLE, SqlType, TableDefinition, TableName, \
    escape_string_literal, \
    HyperException

import tableauserverclient as TSC

key_path = "C:/Users/angelinat/Documents/GCP/service_account.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

#########################################################################################################
                            # export data to storage bucket
#########################################################################################################

def export_to_bucket(project, dataset_id, table_id, export_bucket, file_format):
    filename_pattern = f"{table_id}_*.{file_format}"
    export_bucket_name = f"{export_bucket}/{table_id}"

    client = bigquery.Client()
    destination_uri = f"gs://{export_bucket_name}/{filename_pattern}"
    dataset_ref = client.dataset(dataset_id, project=project)
    table_ref = dataset_ref.table(table_id)

    print("Data Export from BigQuery to bucket has started...")
    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        location="EU",    # Location must match that of the source table.
    )
    extract_job.result()  # Waits for job to complete.

    print(
        f"Exported {project}:{dataset_id}.{table_id} to {destination_uri}"
    )

#######################################################################################################
                            # downlaod data from google storage bucket
#######################################################################################################

def download_bucket_files(download_bucket_name, download_folder, download_file, file_format):
    client = storage.Client()

    # Create this folder locally
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # Retrieve all blobs with a prefix matching the folder
    bucket = client.get_bucket(download_bucket_name)
    print(bucket)
    blobs = list(bucket.list_blobs(prefix=download_file, delimiter='/'))
    for blob in blobs:
        if blob.name.endswith(file_format):    # download only relevant file types
            destination_uri = f'{download_folder}/{blob.name}'
            blob.download_to_filename(destination_uri)
            print(f'{blob.name} downloaded successfully')

#######################################################################################################
                            # union downloaded csv
#######################################################################################################

def union_data(file_name, file_path, save_path, file_format):
    initial_file = f'{file_name}_000000000000.{file_format}'
    merged_file = f"{file_name}.{file_format}"
    path_to_csv = save_path + merged_file

    if os.path.isfile(path_to_csv):   # delete previously created file if it exists
        os.remove(path_to_csv)
        print('Removed old file')
    else:
        pass

    # create new unioned file
    print('Creating new unioned file...')
    with open(path_to_csv,  'a', newline='\n', encoding='utf8') as singleFile:
        # new line in python 3 fixes unix/windows line encodings

        for csv in glob(file_path + f'{file_name}_*.csv'):
            csv_name = csv[csv.find('\\')+1:]

            if csv_name == merged_file:
                pass

            elif csv_name == initial_file:             # write the header + content of first file
                for line in open(csv, 'r'):
                    singleFile.write(line)

            else:
                with open(csv, 'r') as file:           # skip header for subsequent files
                    next(file)                         # lines = f.readlines()[1:] # alternative approach
                    for line in file:
                        singleFile.write(line)

            print(f'{csv} loaded successfully')
            os.remove(csv)
    print(f'{path_to_csv} created')

#######################################################################################################
                            # create hyper file structure
#######################################################################################################

# map sql types for hyper schema
def get_sql_types():
    sql_mapping = {
        'SqlType.text()': SqlType.text(),
        'SqlType.bool()': SqlType.bool(),
        'SqlType.int()': SqlType.int(),
        'SqlType.big_int()': SqlType.big_int(),
        'SqlType.small_int()': SqlType.small_int(),
        'SqlType.double()': SqlType.double(),
        'SqlType.date()': SqlType.date(),
        'SqlType.timestamp()': SqlType.timestamp(),
        'SqlType.timestamp_tz()': SqlType.timestamp_tz(),
        'SqlType.geography()': SqlType.geography(),
        'SqlType.interval()': SqlType.interval(),
        'SqlType.json()': SqlType.json(),
        'SqlType.oid()': SqlType.oid(),
        'SqlType.bytes()': SqlType.bytes()
    }
    return sql_mapping
sql_type = get_sql_types()


# define the table
hyper_table = TableDefinition(
    table_name=TableName("Extract", "Extract")
    ) # the single table must be called "Extract" and published in "Extract" schema for TSC to be able to publish on Server

# add the column names and column types
def define_hyper_schema(path_to_json):
    with open(path_to_json, 'r') as jf:
        data = json.load(jf)

        for line in data:
            field_name = line['name']
            field_type = sql_type[line['type']]  # sql_type must be recognized as class object and not STRING
            field_mode = line['mode']

            column = TableDefinition.Column(field_name, field_type, NULLABLE)
            hyper_table.add_column(column)

#######################################################################################################
                            # add content to hyper file
#######################################################################################################

def create_hyper_file_from_csv(full_path_to_csv, file_name):
    print("Load data from unioned CSV into new Hyper file")

    # create a hyper file variables
    hyper_file = f"{file_name}.hyper"
    path_to_database = Path(hyper_file) # path where hyper file will be created

    # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/processsettings.html).
    process_parameters = {
        "log_file_max_count": "2",      # Limits the number of Hyper event log files to two
        "log_file_size_limit": "100M"   # Limits the size of Hyper event log files to 100 megabyte
    }

    # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/connectionsettings.html).
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:
        connection_parameters = {"lc_time": "en_GB"}

        with Connection(endpoint=hyper.endpoint,
                        database=path_to_database,
                        create_mode=CreateMode.CREATE_AND_REPLACE,      # re-create the hyper file schema.
                        parameters=connection_parameters) as connection:

            # create 'Extract' schema
            connection.catalog.create_schema(schema=hyper_table.table_name.schema_name)
            connection.catalog.create_table(table_definition=hyper_table)

            # pick up the data
            count_in_hyper_table = connection.execute_command(
                command=f"COPY {hyper_table.table_name} "
                        f"from {escape_string_literal(full_path_to_csv)} "
                        f"with "
                        f"(format csv, NULL '', delimiter ',', header)"
                    )

            print(f"The number of rows in table {hyper_table.table_name} is {count_in_hyper_table}.")
        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")

#######################################################################################################
                            # publish hyper file to Tableau Server
#######################################################################################################

# TSC API version must be tableauserverclient==0.11.0 for huge hyper extracts to work

def publish_to_server(tableau_server_link, tableau_user, user_password, project_name, hyper_file):
    tableau_auth = TSC.TableauAuth(tableau_user, user_password)
    server = TSC.Server(tableau_server_link, use_server_version=True)

    path_to_hyper_file = Path(hyper_file)

    with server.auth.sign_in(tableau_auth):
        publish_mode = TSC.Server.PublishMode.Overwrite

        # Get project_id from project_name
        all_projects, pagination_item = server.projects.get()
        project_id = None

        for project in TSC.Pager(server.projects):
            if project.name == project_name:
                project_id = project.id

        # Create the datasource object with the project_id
        datasource = TSC.DatasourceItem(project_id)
        print(f"Publishing {hyper_file} to {project_name}...")

        datasource = server.datasources.publish(datasource, path_to_hyper_file, publish_mode)
        print(f"Publishing of datasource '{hyper_file}' complete. Datasource ID: {datasource.id}")


if __name__ == '__main__':
    export_to_bucket("bigquery-public-data", "hacker_news", "stories", "hyper_extracts", "csv")
    download_bucket_files("hyper_extracts", "C:/Users/angelinat/Documents/data", "stories", "csv")
    union_data("stories", "C:/Users/angelinat/Documents/data", "C:/Users/angelinat/Documents/data/merged", "csv")
    define_hyper_schema('stories_schema.json')
    try:
        create_hyper_file_from_csv("C:/Users/angelinat/Documents/data/merged/stories.csv", "stories_extract")
    except HyperException as ex:
        print(ex)
        exit(1)
    publish_to_server('tableau_server_link', 'tableau_user', 'user_password', 'project_name', 'stories_extract.hyper')
