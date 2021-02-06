SELECT
ds.project_id          	as project_id,
prj.name               	as project_name,
ds.id                  	as datasource_id,
ds.name                	as datasource_name,
ds.repository_url,
ds.parent_workbook_id  	as workbook_id,
w.name                 	as workbook_name,
sysus.name             	as datasource_owner,
w.refreshable_extracts 	as refreshable_extracts,
w.extracts_refreshed_at,
w.last_published_at     as wbk_last_published,
dc.dbclass              as embedded_connection_type,
dc.dbname               as embedded_connection_name,
ds.last_published_at    as connection_last_published,
dc.username             as credentials_used,
dc.password             as embedded_password,
dc.owner_type           as connection_type

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
	AND ds.repository_url LIKE '%embedded%'
	AND dc.dbclass != 'sqlproxy'

ORDER BY
ds.project_id,
ds.last_published_at DESC
