import tableauserverclient as TSC
import datetime as dt

now = dt.datetime.today().strftime('%Y-%m-%d %H-%M-%S')
print(now)


def reassign_wbks_from_owner(tableau_server, tableau_user, user_password, site_name, owner, new_owner):

    '''
        Args:
            tableau_server --> tableau server link
            tableau_user --> tableau admin user
            user_password --> tableau admin password
            site name --> Tableau site
            owner --> original workbook owner
            new_owner --> new workbook owner
    '''

    # authenticate with Tableau Server
    tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
    server = TSC.Server(tableau_server, use_server_version=True)

    with server.auth.sign_in(tableau_auth):
        all_users, pagination_item = server.users.get()     # get all users on site
        user_names = [user.name for user in all_users]
        user_details = {user.name: user.id for user in all_users}

        for u in user_names:
            if u == owner:
                # get all workbooks of the given user
                server.users.populate_workbooks(all_users[0])
                print(user_details)

                # find the user_id of the new owner
                new_owner_id = ''
                for k, v in user_details.items():
                    new_owner_id = v

                # compile a list to re-assign
                wbks_to_reassign = {wbk.name: wbk.id for wbk in all_users[0].workbooks}
                print(f"There are {len(wbks_to_reassign)} workbooks by {owner}")
                print(f"{[k for k in wbks_to_reassign.keys()]}")

                # re-assign the workbooks
                for wbk_name, wbk_id in wbks_to_reassign.items():
                    workbook = server.workbooks.get_by_id(wbk_id)
                    workbook.owner_id = new_owner_id
                    server.workbooks.update(workbook)
                    print(f'Re-assinged {wbk_name} to {new_owner}')


