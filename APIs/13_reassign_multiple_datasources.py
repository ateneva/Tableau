
import tableauserverclient as TSC
import json
from pathlib import Path
from glob import glob
import datetime as dt

now = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print(now)

tableau_server = 'tableau server link'
tableau_user = 'tableau admin username'
user_password = 'tableau admin credentials'
site_name = ''
# if you're connecting to the default site, pass empty string in site_name

tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
server = TSC.Server(tableau_server, use_server_version=True)


def get_owner_ids(old_user, new_user):
    with server.auth.sign_in(tableau_auth):
        all_users, pagination_item = server.users.get()
        old_owner_id = [user.id for user in all_users if user.name == old_user]
        new_owner_id = [user.id for user in all_users if user.name == new_user]
        user_ids = tuple(old_owner_id + new_owner_id)
        print(user_ids)
    return user_ids


def reassign_datasets(published_project, user_ids):
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

            old_owner_id = user_ids[0]
            new_owner_id = user_ids[1]
            if dataset.project_name == published_project:
                dataset.owner_id = new_owner_id
                server.datasources.update(dataset)
                print(f'Re-assigned {dataset.name} from {dataset.project_name} from {old_owner_id} to {new_owner_id}')

                # owner re-assignment automatically resets credentials for Live datasources, so these need updating
                if not dataset.has_extracts:
                    print(f'Re-enter database credentials for datasource {dataset.name}')
