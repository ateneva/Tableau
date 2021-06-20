
# Install & Configure Tableau Server

## Activate and Register Tableau 
```
tsm licenses activate -k <product key>
tsm register --template > /path/to/<registration_file>.json
tsm register --file /path/to/<registration_file>.json
{
    "zip" : "97403",
    "country" : "USA",
    "city" : "Springfield",
    "last_name" : "Simpson",
    "industry" : "Energy",
    "eula" : "yes",
    "title" : "Safety Inspection Engineer",
    "phone" : "5558675309",
    "company" : "Example",
    "state" : "OR",
    "department" : "Engineering",
    "first_name" : "Homer",
    "email" : "homer@example.com"
}
```

## Configure Initial Node Settings
```
tsm settings import -f path-to-file.json
{
"configEntities": {
"identityStore": {
                  "_type": "identityStoreType",
                  "type": "local"
                   }
                     }
 }
```

## Initialize & Start Tableau Server
```
tsm initialize --start-server --request-timeout 1800
tsm initialize --request-timeout 1800
tsm start --request-timeout 900
```

## Add Admin account
```
tabcmd initialuser --server http://localhost --username "<new-admin-username>"
```

## Configure proxy so that Tableau Server is accessible from the internet
```
# review configuration
tsm configuration get -k gateway.trusted
tsm configuration get -k gateway.public.host 
tsm configuration get -k gateway.public.port

# update configuration
tsm configuration set -k gateway.trusted -v "server_ip_address"
tsm configuration set -k gateway.public.host -v "name"

# port must be set tp 443 if the proxy server is using SSL
tsm configuration set -k gateway.public.port -v 443
```


## Perform Full Backup & Restore
```
# export topology and configuration data
tsm settings export -f <filename>.json 
tsm settings export --output-config-file <path/to/output_file.json> 

# backup repository and file store data
tsm maintenance backup -f <filename>.tsbak -d `
C:\ProgramData\Tableau\Tableau Server\data\tabsvc\files\backups\<filename>.tsbak


# import topology and configuration data
tsm settings import -f <filename>.json
tsm settings import --import-config-file <path/to/import_file.json>

# restore backup data
tsm stop
tsm maintenance restore --file <file_name>
tsm start
```

## Activate & Deactivate licenses
```
tsm licenses activate --license-key <product-key>
tsm licenses deactivate --license-key <product-key>
tsm licenses list
```

## Review and Update TSM configuration options
```
# view the current server configuration and topology
tsm configuration get --key <config.key> [global options]

# set server name and IP address
tsm configuration set -k gateway.public.host -v "name"
tsm configuration set -k gateway.trusted -v "server_ip_address"

# increase timeout limit for datasource refreshes
tsm configuration get -k backgrounder.querylimit
tsm configuration set -k backgrounder.querylimit -v 10800 
```
https://help.tableau.com/current/server/en-us/cli_configuration-set_tsm.htm 


## Allow access to the PostgreSQL repository
```
tsm data-access repository-access enable --repository-username readonly --repository-password <PASSWORD>
```

## Test SMTP connection for the configured email (as of 2020.2)
```
tsm email test-smtp-connection
```

## Review and Apply pending changes
```
tsm pending-changes apply
tsm pending-changes discard [options]
tsm pending-changes list [options]
```

## Start and Stop Tableau Server
```
tsm start
tsm stop
tsm restart
tsm status
```

## Review TSM help commands
```
# list all top-level commands or categories
tsm help command     
tsm help <category> tsm help authentication.
tsm help <category> <command> tsm help authentication open-id
```

# Uninstall Tableau Server

If you want to complete remove Tableau Server from a computer, you can use a script provided by Tableau to remove Tableau Server and all related files.

This removes all data as well as server components, so should only be done if you know you want to reset the computer to a pre-Tableau state.  
    
- Deactivate any active product keys 
  `tsm licenses deactivate -k <product_key>`
        
- Navigate to the path > `C:\Program Files\Tableau\Tableau Server\packages\scripts.<version_code>\`
        
- Run through cmd as administrator 
  `tableau-server-obliterate.cmd -a -y -y -y`
        
    - If you have a multi-node installation, run the tableau-server-obliterate script on each node in the cluster

- Restart each computer you ran the tableau-server-obliterate script on

