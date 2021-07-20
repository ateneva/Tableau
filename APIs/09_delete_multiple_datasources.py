import tableauserverclient as TSC
import json
from pathlib import Path
from glob import glob
import datetime as dt

now = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print(now)


def delete_datasets_from_project(tableau_server, tableau_user, user_password, site_name, published_project):
    # if you're connecting to the default site, pass empty string in site_name

    tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
    server = TSC.Server(tableau_server, use_server_version=True)
    req_option = TSC.RequestOptions(pagenumber=1, pagesize=1000)

    with server.auth.sign_in(tableau_auth):
        published, pagination_item = server.datasources.get(req_option)
        for dataset in published:
            server.datasources.populate_connections(dataset)
            connection = dataset.connections[0]

            dataset_config = {
                "dataset_id": dataset.id,
                "dataset_name": dataset.name,
                "dataset_type": dataset.datasource_type,
                "certified": dataset.certified,
                "certification_note": dataset.certification_note,
                "dataset_tags": list(dataset.tags),
                "published on": dataset.content_url,
                "dataset project": dataset.project_name,
                "dataset_connection_type": connection.connection_type,
                "connection_id": connection.id,
                "connection_address": connection.server_address,
                "connection_username": connection.username,
                "first_created": str(dataset.created_at),
                "last_updated": str(dataset.updated_at)
                }

            # delete datasets from specified project
            if dataset.project_name == published_project and connection.connection_type == 'sqlserver':
                print(f'Deleting {dataset.name} from {dataset.project_name} connected to {connection.connection_type} and last updated on {dataset.updated_at}')
                server.datasources.delete(dataset.id)

