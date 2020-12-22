import tableauserverclient as TSC
import tableaudocumentapi as DoC
import json
from pathlib import Path
from glob import glob
import datetime as dt
import credentials as cr

now = dt.datetime.today().strftime('%Y%m%d%H%M%S')
print(now)

# user needs to have permissions to download datasources
tableau_auth = TSC.TableauAuth(cr.tableau_user, cr.user_password)
server = TSC.Server(cr.tableau_server_link, use_server_version=True)

# call the sign-in method with the auth object
server.auth.sign_in(tableau_auth)
under_review = ['customer', '2018-19 Arsenal Player Stats']
download_path = 'C:/Users/angelinat/Desktop/'

for trigger in under_review:
    # pagination item can only list the first 100, therefore use request options to filter for specific dataset
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, trigger))

    with server.auth.sign_in(tableau_auth):
        published, pagination_item = server.datasources.get(req_option)
        for dataset in published:
            server.datasources.populate_connections(dataset)            # get the connection information
            connection = dataset.connections[0]

            tableau_schema = []
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
                "last_updated": str(dataset.updated_at),
                "dataset_fields": []
            }

            # download datasources under review
            print(f'Downloading {dataset.name} from {dataset.project_name} last updated on {dataset.updated_at}')
            server.datasources.download(dataset.id, download_path, include_extract=True, no_extract=None)

            ###########################################################################################################
            # read the setup of the dowloaded dataset
            datasource_extension = ''
            for ds_file in glob(download_path + '*'):
                if Path(ds_file).suffix in ['.tdsx', '.hyper']:
                    datasource_extension = Path(ds_file).suffix

            downloaded = DoC.Datasource.from_file(download_path + dataset.name + datasource_extension)
            print(f'{len(downloaded.fields)} total fields in this datasource')

            # retrieve dataset connection setup and query dataset fields
            with open(download_path + str(now) + '_' + dataset.name + '.json', 'a') as f:
                for k, v in dataset_config.items():
                    if k == 'dataset_fields':                               # update dataset fields audit
                        for count, field in enumerate(downloaded.fields.values()):
                            fields_dict = {
                                "field_id": field.id,                       # remote field name
                                "field_name": field.name,                   # name in tableau datasource
                                "field_datatype": field.datatype,
                                "field_default_aggregation": field.default_aggregation,  # sum, avg, min, max
                                "field_calculation": field.calculation,     # formula of calculated field (if any)
                                "field_description": field.description      # field comment
                            }
                            v.append(fields_dict)
                print(dataset_config)
                tableau_schema.append(dataset_config)

                json.dump(tableau_schema, f, indent=4)  # generate schema to ingest
