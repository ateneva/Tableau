import sys
import os
from os.path import dirname

import json
from google.cloud import bigquery
from google.cloud import storage

from pathlib import Path
from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NULLABLE, SqlType, TableDefinition, TableName, \
    escape_string_literal, \
    HyperException

import tableauserverclient as TSC

# key below is used for local testing purposes
#key_path = '/Users/angelina_teneva/Documents/repos/python_handy/angelinat_service_account.json'
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

#########################################################################################################
                            # export data to storage bucket
#########################################################################################################

def export_to_bucket(project, dataset_id, table_id, export_bucket, file_format):
    export_bucket_name = f"{export_bucket}/TABLEAU_HYPER/{table_id}"
    filename_pattern = f"{table_id}_*.{file_format}"

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
                            # create hyper file structure
#######################################################################################################

# map sql types so that values from hyper schema are recognized as hyper objects
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
        )
        # the single table must be called "Extract" and published in "Extract" schema
        # for TSC to be able to publish on Server

def define_hyper_schema(hyper_bucket_name, hyper_schema):
    print('Defining hyper file structure...')
    client = storage.Client()
    hyper_schema_folder = dirname(os.path.abspath(__file__)) + '/'          # retrieve current directory
    hyper_schema_name = hyper_schema.split('/')[2]                          # retrieve pure name of hyper schema
    hyper_schema_full_path = f'{hyper_schema_folder}{hyper_schema_name}'

    # retrieve parent directory
        # dirname(dirname(os.path.abspath(__file__))) + '/'

    # download hyper schema from GCP storage
    bucket = client.get_bucket(hyper_bucket_name)
    blob = bucket.blob(hyper_schema)
    blob.download_to_filename(hyper_schema_full_path)

    # add the column names and column types
    with open(hyper_schema_full_path, 'r') as jf:
        data = json.load(jf)

        for line in data:
            field_name = line['name']
            field_type = sql_type[line['type']]  # sql_type must be recognized as class object and not STRING
            field_mode = line['mode']

            column = TableDefinition.Column(field_name, field_type, NULLABLE)
            hyper_table.add_column(column)

    print('Hyper structure defined!')
#######################################################################################################
                            # add content to hyper file
#######################################################################################################

def create_hyper_file_from_csv(file_name, download_bucket_name, hyper_file_name):
    print("Loading data into new Hyper file...")

    # retrieve current directory
    download_folder = dirname(os.path.abspath(__file__)) + '/'

    hyper_file = f"{hyper_file_name}.hyper"
    path_to_hyper_file = Path(hyper_file)  # path where hyper file will be created

    # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/processsettings.html).
    process_parameters = {
        "log_file_max_count": "2",         # Limits the number of Hyper event log files to two
        "log_file_size_limit": "100M"      # Limits the size of Hyper event log files to 100 megabyte
    }

    # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/connectionsettings.html).
    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:
        connection_parameters = {"lc_time": "en_GB"}

        with Connection(endpoint=hyper.endpoint,
                        database=path_to_hyper_file,
                        create_mode=CreateMode.CREATE_AND_REPLACE,      # re-create the hyper file schema.
                        parameters=connection_parameters) as connection:

            # create 'Extract' schema in hyper
            connection.catalog.create_schema(schema=hyper_table.table_name.schema_name)
            connection.catalog.create_table(table_definition=hyper_table)

            # pick up the data from GCP storage and add it to hyper file by file
            client = storage.Client()
            bucket = client.get_bucket(download_bucket_name)
            files_to_download = f'TABLEAU_HYPER/{file_name}/'
            print(files_to_download)

            # only retrieve files that match the filename pattern
            blobs = list(bucket.list_blobs(prefix=files_to_download, delimiter='/'))
            print(f'Files to process: {len([blob for blob in blobs if blob.name.endswith(".csv")])}')
            total_rows = 0

            for blob in blobs:
                if blob.name.endswith(".csv"):  # make sure only csv files are dowwloaded
                    csv_name = blob.name.split("/")[2]
                    csv = f'{download_folder}{csv_name}'
                    blob.download_to_filename(csv)
                    print(f'{csv_name} downloaded successfully')

                    count_in_hyper_table = connection.execute_command(     # add to hyper file
                        command=f"COPY {hyper_table.table_name} "
                                f"from {escape_string_literal(csv_name)} "
                                f"with "
                                f"(format csv, NULL '', delimiter ',', header)"
                            )

                    total_rows += count_in_hyper_table
                    print(f"Rows in file {csv_name} is {count_in_hyper_table}.")
                    print(f'{csv_name} successfully processed')
                    print(f"The number of rows in table {hyper_table.table_name} is {total_rows}.")

                    os.remove(csv)                                         # delete local file
                    blob.delete()                                          # delete file from storage bucket
        print("The connection to the Hyper file has been closed.")
    print("The Hyper process has been shut down.")

#######################################################################################################
                            # publish hyper file to Tableau Server
#######################################################################################################

# TSC API version must be tableauserverclient==0.11.0 for huge hyper extracts to work

def publish_to_server(tableau_server_link, tableau_user, user_password, project_name, hyper_extract):
    tableau_auth = TSC.TableauAuth(tableau_user, user_password)
    server = TSC.Server(tableau_server_link, use_server_version=True)

    path_to_hyper_file = Path(hyper_extract)

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
        print(f"Publishing {hyper_extract} to {project_name}...")

        datasource = server.datasources.publish(datasource, path_to_hyper_file, publish_mode)
        print(f"Publishing of datasource '{hyper_extract}' complete. Datasource ID: {datasource.id}")

if __name__ == '__main__':
    export = json.loads(sys.argv[1])
    export_to_bucket(export['project'], export['dataset_id'], export['table_id'], export['export_bucket'], export['file_format'])
    #export_args = (project, dataset_id, table_id, export_bucket, file_format)
    #export_to_bucket('BQ project', 'BQ dataset', 'BQ table', 'GCS storage bucket', 'csv')

    hyper_schema_details = json.loads(sys.argv[1])
    define_hyper_schema(hyper_schema_details['hyper_bucket_name'], hyper_schema_details['hyper_schema'])
    #schema_args = hyper_bucket_name, hyper_schema
    #define_hyper_schema('GCS storage bucket', 'hyper/daily_update/data.json')

    hyper_file_payload = json.loads(sys.argv[1])
    #hyper_file_payload_args = file_name, download_bucket_name

    try:
        create_hyper_file_from_csv(hyper_file_payload['file_name'], hyper_file_payload['download_bucket_name'], hyper_file_payload['hyper_file_name'])
    except HyperException as ex:
        print(ex)
        exit(1)

    publish_details = json.loads(sys.argv[1])
    publish_to_server(publish_details['tableau_server_link'], publish_details['tableau_user'], publish_details['user_password'], publish_details['project_name'], publish_details['hyper_extract'])

    #publish_details_args = tableau_server_link, tableau_user, user_password, project_name, hyper_extract
