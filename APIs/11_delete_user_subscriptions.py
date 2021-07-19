import tableauserverclient as TSC
import json
import datetime as dt

now = dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
print(now)


def delete_subscriptions(tableau_server, tableau_user, user_password, site_name, subs_user):
    # if you're connecting to the default site, pass empty string in site_name

    '''
        Args:
            tableau_server --> tableau server link
            tableau_user --> tableau admin user
            user_password --> tableau admin password
            site name --> Tableau site
            subs_user --> user whose subscriptions we want to delete
    '''

    tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
    server = TSC.Server(tableau_server, use_server_version=True)

    with server.auth.sign_in(tableau_auth):

        # get all users
        all_users, pagination_item = server.users.get()

        # get users and user ids
        user_names = [user.name for user in all_users if user.name == subs_user]
        user_ids = [user.id for user in all_users if user.name == subs_user]
        user_details = dict(zip(user_names, user_ids))
        sub_user_id = user_details[subs_user]
        print(sub_user_id)

        # identify user subscriptions that should be deleted
        subs = server.subscriptions.get(req_options=None)[0]
        subs_to_delete = []
        for s in subs:
            if s.user_id == sub_user_id:
                subs_to_delete.append(s.id)

        print(f'{subs_user} has {len(subs_to_delete)} subscriptions, with IDs: {subs_to_delete}')

        # delete identified subscriptions
        for s in subs_to_delete:
            server.subscriptions.delete(s)
            subs_to_delete.remove(s)

        print(f'{len(subs_to_delete)} subscriptions left for this user')
