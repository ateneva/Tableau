import tableauserverclient as TSC
import datetime as dt

now = dt.datetime.today().strftime('%Y-%m-%d %H-%M-%S')
print(now)

tableau_server = 'tableau server link'
tableau_user = 'tableau admin username'
user_password = 'tableau admin credentials'
site_name = 'tableau site name'
download_path = 'C:/Users/angelinat/Desktop/TESTS/'

# if you're connecting to the default site, pass empty string in site_name
# download path - e.g. 'C:/Users/angelinat/Desktop/TESTS/'

# authenticate with Tableau Server
tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
server = TSC.Server(tableau_server, use_server_version=True)


def download_wbks_from_owner(owners):
    '''
    Args: expects a list of owner user names
    '''

    with server.auth.sign_in(tableau_auth):
        all_users, pagination_item = server.users.get()     # get all users on site
        user_names = {user.name: user.id for user in all_users}

        for owner in owners:
            own_id = user_names[owner]

            # populate all workbooks whih the person owns or has access to
            server.users.populate_workbooks(all_users[0])
            all_wbks = {wbk.name: wbk.id for wbk in all_users[0].workbooks}

            # download the workbooks
            user_wbks = []
            for wbk_name, wbk_id in all_wbks.items():
                workbook = server.workbooks.get_by_id(wbk_id)
                if workbook.owner_id == own_id:
                    user_wbks.append(wbk_name)
                    server.workbooks.download(wbk_id, filepath=download_path)
                    print(f'Downloaded {wbk_name}')

            print(f"There are {len(user_wbks)} workbooks by {owner}: {user_wbks}")


def download_wbks_from_project(project):
    with server.auth.sign_in(tableau_auth):

        # page through all the workbooks on the Server and keep the relevant ones
        wbks_to_review = {wbk.name: wbk.id for wbk in TSC.Pager(server.workbooks) if wbk.project_name == project}
        print(f"{[k for k in wbks_to_review.keys()]}")

        # download the workbooks
        for wbk_name, wbk_id in wbks_to_review.items():
            server.workbooks.download(wbk_id, filepath=download_path)
            print(f'Downloaded {wbk_name}')


def download_wbks_connected_to(datasource):
    with server.auth.sign_in(tableau_auth):
        all_workbooks = {wbk.name: wbk.id for wbk in TSC.Pager(server.workbooks)}

        # compile a list to download
        wbks_to_review = {}
        for wb in all_workbooks.values():
            print(f'Checking workbook connections for {wb}')
            workbook = server.workbooks.get_by_id(wb)
            server.workbooks.populate_connections(workbook)

            connections_info = [connection.datasource_name for connection in workbook.connections]
            if datasource in connections_info:
                wbks_to_review.update({workbook.name: workbook.id})

        print(f"There are {len(wbks_to_review)} workbooks connected to {datasource}")

        # download the workbooks
        for wbk_name, wbk_id in wbks_to_review.items():
            server.workbooks.download(wbk_id, filepath=download_path)
            print(f'Downloaded {wbk_name}')
