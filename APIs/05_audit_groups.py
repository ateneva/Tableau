import tableauserverclient as TSC
import json
import datetime as dt
import credentials as cr

now = dt.datetime.today().strftime('%Y%m%d%H%M%S')
print(now)

tableau_auth = TSC.TableauAuth(cr.tableau_user, cr.user_password)
server = TSC.Server(cr.tableau_server_link, use_server_version=True)
file_path = 'C:/Users/angelinat/Desktop/'

with server.auth.sign_in(tableau_auth):
    all_groups, pagination_item = server.groups.get()

    with open(file_path + str(now) + '_' + 'audited_groups.json', 'a') as f:
        groups_ls = []

        for group in all_groups:
            my_groups = ['Marketing', 'Management', 'Sales', 'Finance']

            if group.name in my_groups:
                pub_group = server.groups.populate_users(group)
                group_users = [user.name for user in group.users]

                audited_groups = {
                    "group_id": group.id,
                    "group_name": group.name,
                    "group_users": group_users
                }
                print(audited_groups)
                groups_ls.append(audited_groups)

        json.dump(groups_ls, f, indent=2)
