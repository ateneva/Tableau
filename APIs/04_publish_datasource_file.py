# tableauserverclient==0.11
import tableauserverclient as TSC
from pathlib import Path

def publish_multiple_datasources(tableau_server, tableau_user, user_password, site_name, project_name, local_folder):

    # authenticate with the correct site
    tableau_auth = TSC.TableauAuth(tableau_user, user_password, site_id=site_name)
    server = TSC.Server(tableau_server, use_server_version=True)
    path_to_datasource = Path(local_folder)

    # get all projects available on the site and identify the ID against the project name
    with server.auth.sign_in(tableau_auth):
        all_sites, pagination_item = server.sites.get()
        for site in all_sites:
            if site_name == site.name:
                all_projects, pagination_item = server.projects.get()
                print(site.id, site.name, site.content_url, site.state)

                project_id = None
                for project in TSC.Pager(server.projects):
                    if project.name == project_name:
                        project_id = project.id
                        print(project_name)

                # publish all datasource files in a specified local path
                files_in_path = path_to_datasource.iterdir()
                for item in files_in_path:
                    if item.is_file():
                        item_name = item.name
                        if item_name.endswith('.tdsx') or item_name.endswith('.hyper'):
                            print(item.name)

                            # define publishing variables
                            datasource = TSC.DatasourceItem(project_id)
                            publish_path = f'{path_to_datasource}/{item_name}'
                            publish_mode = TSC.Server.PublishMode.Overwrite

                            # publish the datasource
                            datasource_= server.datasources.publish(datasource, publish_path, publish_mode)
                            print(f"Published {item_name} to {project_name}.")
