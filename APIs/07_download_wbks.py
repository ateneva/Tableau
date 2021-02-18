import tableauserverclient as TSC
import datetime as dt

now = dt.datetime.today().strftime('%Y-%m-%d %H-%M-%S')
print(now)

def download_wbks_from_owner(tableau_server, tableau_user, user_password, site_name, download_path, owner):
    # if you're connecting to the default site, pass empty string in site_name
    # download path - e.g. 'C:/Users/angelinat/Desktop/TESTS/'

    # authenticate with Tableau Server
    tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
    server = TSC.Server(tableau_server, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        all_users, pagination_item = server.users.get()     # get all users on site
        user_names = [user.name for user in all_users]

        for u in user_names:
            if u == owner:
                # get all workbooks of the given user
                server.users.populate_workbooks(all_users[0])

                # compile a list to download
                wbks_to_download = {wbk.name: wbk.id for wbk in all_users[0].workbooks}
                print(f"There are {len(wbks_to_download)} workbooks by {owner}")
                print(f"{[k for k in wbks_to_download.keys()]}")

                # download the workbooks
                for wbk_name, wbk_id in wbks_to_download.items():
                    server.workbooks.download(wbk_id, filepath=download_path)
                    print(f'Downloaded {wbk_name}')


def download_wbks_from_project(tableau_server, tableau_user, user_password, site_name, download_path, project):
    # authenticate with Tableau Server
    tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
    server = TSC.Server(tableau_server, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        # page through all the workbooks on the Server and keep the relevant ones
        wbks_to_review = {wbk.name: wbk.id for wbk in TSC.Pager(server.workbooks) if wbk.project_name == project}
        print(f"{[k for k in wbks_to_review.keys()]}")

        # download the workbooks
        for wbk_name, wbk_id in wbks_to_review.items():
            server.workbooks.download(wbk_id, filepath=download_path)
            print(f'Downloaded {wbk_name}')


def download_wbks_connected_to(tableau_server, tableau_user, user_password, site_name, download_path, datasource):
    # authenticate with Tableau Server
    tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
    server = TSC.Server(tableau_server, use_server_version=True)

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
