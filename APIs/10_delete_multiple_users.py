import tableauserverclient as TSC
import datetime as dt

now = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print(now)

tableau_server = 'tableau server link'
tableau_user = 'tableau admin username'
user_password = 'tableau admin credentials'
site_name = 'tableau site name'
# if you're connecting to the default site, pass empty string in site_name

tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
server = TSC.Server(tableau_server, use_server_version=True)

# a user can only be deleted if they have no content under their name


def identify_unlicensed():
    # get all users & populate their workbooks
    with server.auth.sign_in(tableau_auth):
        all_users, pagination_item = server.users.get()

        users_to_delete = {user.name: user.id for user in all_users if user.site_role == 'Unlicensed'}
        print(f"There are {len(users_to_delete.keys())} unlicensed users on site")

        for u in users_to_delete.keys():
            print(u)
    return users_to_delete


def identify_user_wbks(users_to_delete):
    with server.auth.sign_in(tableau_auth):
        all_users, pagination_item = server.users.get()     # get all users on site
        unlicensed = [k for k in users_to_delete.keys()]
        owner_ids = [v for v in users_to_delete.values()]
        print(unlicensed)

        # get all workbooks of the given user
        server.users.populate_workbooks(all_users[0])

        # compile a list of all workbooks which the user owns or has permissions to view
        all_wbks = {wbk.name: wbk.id for wbk in all_users[0].workbooks}

        # delete the workbooks that are owned by unlicensed users
        unlicensed_user_wbks = {}
        for wbk_name, wbk_id in all_wbks.items():
            workbook = server.workbooks.get_by_id(wbk_id)
            for own_id in owner_ids:
                if workbook.owner_id == own_id:
                    unlicensed_user_wbks[wbk_name] = wbk_id

        print(f"There are {len(unlicensed_user_wbks)} workbooks by {unlicensed}")
        print(f"{[k for k in unlicensed_user_wbks.keys()]}")
    return unlicensed_user_wbks

# below you can see how to re-assign existing content
# https://github.com/ateneva/Tableau/blob/main/APIs/12_reassign_multiple_workbooks.py


def delete_wbks(unlicensed_user_wbks):
    # sign in
    with server.auth.sign_in(tableau_auth):
        for wbk_name, wbk_id in unlicensed_user_wbks.items():
            print(wbk_id)
            server.workbooks.get_by_id(wbk_id)
            server.workbooks.delete(wbk_id)
            print(f'Deleted wbk_id {wbk_name}')
    print(f'Deleted all workbooks from {[k for k in unlicensed_user_wbks.keys()]}')


def delete_users(users_to_delete):
    # sign in
    with server.auth.sign_in(tableau_auth):

        # delete users
        for u, i in users_to_delete.items():
            server.users.remove(i)
            print(f'{u} removed')

        # check that all unlicensed users have been deleted
        left_unlicensed = [u for u in users_to_delete.keys()]
        print(f"{len(left_unlicensed)} unlicensed users were removed from site")


if __name__ == '__main__':
    identify_user_wbks(identify_unlicensed())
    delete_wbks(identify_user_wbks(identify_unlicensed()))
    delete_users(identify_unlicensed())
