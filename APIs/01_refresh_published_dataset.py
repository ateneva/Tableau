
import tableauserverclient as TSC
import credentials as cr

tableau_auth = TSC.TableauAuth(cr.tableau_user, cr.user_password)
server = TSC.Server(cr.tableau_server_link, use_server_version=True)
server.auth.sign_in(tableau_auth)

refreshes = ['published datasource name(s) as defined on server']
for trigger in refreshes:
    # pagination item can only list the first 100, therefore use request options to filter for the specific dataset
    req_option = TSC.RequestOptions()
    req_option.filter.add(TSC.Filter(TSC.RequestOptions.Field.Name, TSC.RequestOptions.Operator.Equals, trigger))

    with server.auth.sign_in(tableau_auth):
        published, pagination_item = server.datasources.get(req_option)
        print(f'Datasources {pagination_item.total_available} found on site')

        for dataset in published:
            server.datasources.populate_connections(dataset)    # get the connection information
            connection = dataset.connections[0]

            dataset_id = dataset.id
            dataset_name = dataset.name
            dataset_type = dataset.datasource_type
            dataset_connection_type = connection.connection_type
            connection_id = connection.id
            connection_address = connection.server_address
            connection_username = connection.username

            print(
                dataset_id, '|',
                dataset_name, '|',
                dataset_type, '|',
                dataset_connection_type, '|',
                connection_id, '|',
                connection_username, '|',
                connection_address
                  )

            if dataset_type != 'hyper':                        # hyper files can't be refreshed
                server.datasources.refresh(dataset)            # only possible to trigger full refresh
                print(dataset_name, 'was triggered for refresh')
