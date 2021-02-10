import tableauserverclient as TSC
import json
import datetime as dt

now = dt.datetime.today().strftime('%Y%m%d%H%M%S')
print(now)

def audit_groups(tableau_server, tableau_user, user_password, site_name, local_folder, *args):
    # if you're connecting to the default site, pass empty string in site_name

    tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
    server = TSC.Server(tableau_server, use_server_version=True)
    file_path = local_folder   # 'C:/Users/angelinat/Desktop/'

    with server.auth.sign_in(tableau_auth):
        all_groups, pagination_item = server.groups.get()

        audit_file = file_path + str(now) + '_' + 'audited_groups.json'

        with open(audit_file, 'a') as f:
            groups_ls = []

            for group in all_groups:
                my_groups = []
                for a in args:
                    my_groups.append(a)

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
        print(f'Audit Complete! Check {audit_file} for further details...:\n')
