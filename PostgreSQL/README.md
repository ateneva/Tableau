
##  Please note:

- capturing metadata via PostgreSQL 'workgroup' DB is only possible **if you manage your own Tableau Server instance** 


- Tableau Server must be **configured to collect metadata**
    - port 8060 must be opened on the repository node
    - access to the repository must be enabled
    - password must be set for the readonly user
 
```
tsm data-access repository-access enable --repository-username readonly --repository-password <PASSWORD>
```

https://help.tableau.com/current/server/en-us/perf_collect_server_repo.htm
