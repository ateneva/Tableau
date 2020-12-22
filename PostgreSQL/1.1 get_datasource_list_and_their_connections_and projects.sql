SELECT
ds.site_id						as SiteID,
ds.project_id       			as ProjectID,
prj.name            			as ProjectName,
ds.id               			as DatasourceID,
ds.name             			as DatasourceName,
ds.description					as AboutNote,                     -- About description added to TableauServer
ds.first_published_at			as DatasourceFirstPublishedOn,    -- when the datasource was first published to TableauServer
ds.last_published_at			as DatasourceLastPublishedOn,     -- when the datasource was last re-published on TableauServer
ds.refreshable_extracts         as RefreshableExtracts,           -- TRUE/FALSE indicator showing if extracts were set or not
ds.incrementable_extracts       as IncrementableExtracts,         -- TREU/FALSE indocator showing if extracts are incremented or not
ds.extracts_refreshed_at        as LastFullExtractRefreshedOn,    -- timestamp of the last full refresh
ds.extracts_incremented_at      as LastIncrementedExtractOn,      -- timestamp of the last incremental refresh
ds.field_description            AS DatasourceNote,
ds.is_certified                 as IsCertified,                   -- TRUE/FALSE indicator showing if the dataset is certified or not
ds.certifier_details            as CertifiedBy,                   -- display name of the Tableau User who certified it
ds.certification_note           as CertificationNote,
sysus.name          			as DataSourceOwner,         	  -- user who last (re-)published the dataset
dc.id							as ConnectionID,
dc.server						as ConnectionServer,              -- IP to which the connection is made;  NULL in case if bigquery and empty string for Excel and google sheets
dc.dbclass          			as ConnectionDatabaseType,        -- e.g. sqlserver, mysql, postgresql, bigquery, excel-direct, google-sheets. etc
dc.dbname           			as ConnectionDatabaseName,        -- the exact database to which the database is connected --> #if cross-DB connections are used, this will return more than 1 entry
dc.owner_type       			as ConnectionType,                -- e.g. Datasource, Workbook
dc.username         			as CredentialsUsed,               -- the credentials used to connect to the data
dc.password         			as PasswordEmbedded,              -- TRUE/FALSE indicator that shows if password was embedded or not
dc.luid             			as ConnectionUniqueIdentifier,    -- the unique identifer used by the TableauServer Client API
dc.created_at       			as ConnectionFirstCreatedOn,      -- timestamp of the connection creation
dc.updated_at       			as ConnectionLastRevisedOn,	      -- timestamp of the latest connection update
dc.has_extract      			as ConnectionUsesExtract,    	  -- TRUE/FALSE indicator showing if extract is set
dc.caption 						as ConnectionFriendlyName         -- as seen in Desktop Pane; NB use with caution may be manually overwritten by a user

FROM public.datasources ds
INNER JOIN public.data_connections dc
    ON ds.id = dc.datasource_id

INNER JOIN projects prj
    ON ds.project_id = prj.id

INNER JOIN public.users us
    ON ds.owner_id = us.id

INNER JOIN public.system_users sysus
    ON  sysus.id = us.system_user_id

WHERE dc.owner_type = 'Datasource'       -- 'Datasource' represents a published dataset; 'Workbook' means embedded dataset

ORDER BY ds.project_id, ds.name
