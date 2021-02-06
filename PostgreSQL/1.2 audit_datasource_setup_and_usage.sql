WITH DS_SETUP AS (
	SELECT
	ds.site_id                      as site_id,
	s.name                          as site_name,
	ds.project_id                   as project_id,
	prj.name                        as project_name,
	ds.id                           as datasource_id,
	ds.name                         as datasource_name,
	ds.description                  as about_note,                          -- About description added to TableauServer
	ds.first_published_at           as datasource_first_published,          -- when the datasource was first published to TableauServer
	ds.last_published_at            as datasource_last_published,           -- when the datasource was last re-published on TableauServer
	ds.refreshable_extracts         as has_refreshable_extracts,            -- TRUE/FALSE indicator showing if extracts were set or not
	ds.incrementable_extracts       as has_incrementable_extracts,          -- TREU/FALSE indocator showing if extracts are incremented or not
	ds.extracts_refreshed_at        as last_full_extract_refresh,           -- timestamp of the last full refresh
	ds.extracts_incremented_at      as last_incremental_extract_refresh,    -- timestamp of the last incremental refresh
	ds.is_certified                 as is_certified,                        -- TRUE/FALSE indicator showing if the dataset is certified or not
	ds.certifier_details            as certified_by,                        -- display name of the Tableau User who certified it
	ds.certification_note           as certfication_note,
	sysus.name                      as datasource_owner,                    -- user who last (re-)published the dataset
	dc.id                           as datasource_connection_id,
	dc.server                       as datasource_connected_to,             -- IP to which the connection is made;  NULL in case if bigquery and empty string for Excel and google sheets
	dc.dbclass                      as datasource_connection_type,          -- e.g. sqlserver, mysql, postgresql, bigquery, excel-direct, google-sheets. etc
	dc.dbname                       as datasource_connection_name,          -- the exact database to which the database is connected --> #if cross-DB connections are used, this will return more than 1 entry
	dc.owner_type                   as connection_type,                     -- e.g. Datasource, Workbook
	dc.username                     as credentials_used,                    -- the credentials used to connect to the data
	dc.password                     as embedded_password,                   -- TRUE/FALSE indicator that shows if password was embedded or not
	dc.luid                         as connection_luid,						-- the unique identifer used by the TableauServer Client API
	dc.created_at                   as connection_first_created,            -- timestamp of the connection creation
	dc.updated_at                   as connection_last_revised,				-- timestamp of the latest connection update
	dc.has_extract                  as connection_uses_extract,             -- TRUE/FALSE indicator showing if extract is set
	dc.caption                      as connection_friendly_name				-- as seen in Desktop Pane; NB use with caution may be manually overwritten by a user

	FROM public.datasources ds
	INNER JOIN public.data_connections dc
		ON ds.id = dc.datasource_id

	------get datasource site -------------
	INNER JOIN public.sites s
		ON s.id = ds.site_id

	------get datasource project------------
	INNER JOIN projects prj
		ON ds.project_id = prj.id

	------get datasource owner---------------
	INNER JOIN public.users us
		ON ds.owner_id = us.id

	INNER JOIN public.system_users sysus
		ON  sysus.id = us.system_user_id


	WHERE dc.owner_type = 'Datasource'       --'Datasource' represents a published dataset; 'Workbook' means embedded dataset

	ORDER BY
	ds.project_id,
	ds.name
)

, DS_ACCESS as (
	SELECT
	hd.datasource_id,
	max(he.created_at) as last_accessed,
	count(*) 		   as times_accessed

	FROM
	historical_events he,
	hist_datasources hd,
	historical_event_types het

	WHERE he.hist_datasource_id = hd.id
		AND he.historical_event_type_id = het.type_id
		AND (het.name = 'Access Data Source'
				OR het.name = 'Download Data Source')

	GROUP BY
	hd.datasource_id

	ORDER BY datasource_id

	)

, DS_METRICS_AGGREGATIONS AS (
	SELECT
	ma.datasource_id,
	TO_DATE(
		CONCAT(
			CAST(ma.year_index as text), '-',
			CAST(ma.month_index as text), '-',
			CAST(ma.day_index as text), '-'
		), 'YYYY-MM-DD' ) as date_viewed,

	SUM(ma.view_count) 	as num_views
	FROM public.datasource_metrics_aggregations ma
	GROUP BY 1,2
	ORDER BY 2 DESC

)

SELECT
S.*,
S.connection_friendly_name,
A.last_accessed,
A.times_accessed,
M.date_viewed,
M.num_views

FROM DS_SETUP S
LEFT JOIN DS_ACCESS A
	ON S.datasource_id = A.datasource_id

LEFT JOIN DS_METRICS_AGGREGATIONS M
	ON S.datasource_id = M.datasource_id

ORDER BY S.datasource_id
