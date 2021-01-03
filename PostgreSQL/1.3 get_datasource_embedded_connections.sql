SELECT
ds.project_id          	as ProjectID,
prj.name               	as ProjectName,
ds.id                  	as DatasourceID,
ds.name                	as DatasourceName,
ds.parent_workbook_id  	as WorkbookID,
w.name                 	as WorkbookName,
sysus.name             	as DataSourceOwner,
w.refreshable_extracts 	as refreshable_extracts,
w.extracts_refreshed_at,
w.last_published_at    	as wbk_published_on,
dc.server,
dc.dbclass,
dc.dbname,
ds.last_published_at,
dc.username,
dc.password,
dc.owner_type       	as ConnectionType

FROM public.datasources ds
INNER JOIN public.data_connections dc
	ON ds.id = dc.datasource_id

INNER JOIN public.workbooks w
	ON ds.parent_workbook_id = w.id

INNER JOIN projects prj
	ON ds.project_id = prj.id

INNER JOIN public.users us
	ON ds.owner_id = us.id

INNER JOIN public.system_users sysus
	ON  sysus.id = us.system_user_id

WHERE dc.owner_type = 'Workbook'
	--and dc.dbclass = 'sqlserver'

ORDER BY
ds.project_id,
ds.last_published_at DESC
