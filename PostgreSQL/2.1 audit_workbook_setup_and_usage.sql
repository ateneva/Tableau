WITH WORKBOOK_SETUP AS (
    --which workbooks were recently published?
    --who published them and where?
    SELECT
	wb.luid                     as wbk_luid,
	wb.id						as wbk_id,
  	wb.name 					as wbk_name,

  	s.name						as wbk_site_name,
  	prj.name					as wbk_project_name,

  	wb.created_at				as wbk_first_created,
  	wb.updated_at				as wbk_last_updated,
  	wb.owner_id					as wbk_owner_id,
  	sysus.name          		as wbk_owner,
    wb.first_published_at		as wbk_first_published,
    wb.display_tabs				as wbk_display_tabs,
    wb.published_all_sheets     as wbk_published_all_sheets,

    wb.data_engine_extracts		as wbk_connects_to_hyper_extracts,
    wb.refreshable_extracts		as wbk_full_refresh_set,
    wb.extracts_refreshed_at	as wbk_last_fully_refreshed_at,
    wb.incrementable_extracts	as wbk_incremental_refresh_set,
    wb.extracts_incremented_at	as wbk_last_refreshed_incrementally_at,

    wb.last_published_at		as wbk_last_published_at,
    wb.modified_by_user_id		as wbk_last_modified_by,
    wb.revision					as wbk_revision,
    wb.document_version,
    wb.content_version

 	FROM public.workbooks wb

 	------get workbook site -------------
	INNER JOIN public.sites s
		ON s.id = wb.site_id

 	------get project info---------
 	INNER JOIN projects prj
		ON wb.project_id = prj.id

	------get owner info-----------
	INNER JOIN public.users us
		ON wb.owner_id = us.id

	INNER JOIN public.system_users sysus
		ON  sysus.id = us.system_user_id

)

, USED_PUBLISHED_DATASOURCES as (
    --which published datasources do workbooks use?
    SELECT
	ds.project_id          		as project_id,
	ds.parent_workbook_id  		as workbook_id,
	w.name                 		as workbook_name,
	STRING_AGG(ds.name,'--')	as datasources_used

	FROM public.workbooks w
	INNER JOIN public.datasources ds
		ON ds.parent_workbook_id = w.id

	INNER JOIN public.data_connections dc
		ON ds.id = dc.datasource_id

	WHERE dc.owner_type = 'Workbook'
		AND dc.dbclass = 'sqlproxy'

	GROUP BY 1,2,3
)

, USED_FIELDS as (
    --what fields are used per workbook?
    SELECT
	v.workbook_id,
	string_agg(distinct v.fields, ',') as fields_used

	FROM public.views v
	GROUP BY 1
)

, WORKBOOK_USAGE AS (
    -- when was a workbook last viwed and how many times in total?
    -- who ever viewed the workbook?
    SELECT
 	v.workbook_id	 				   as workbook_id,
    STRING_AGG(su.friendly_name, ', ') as viewers,
    STRING_AGG(distinct v.name, ', ')  as views_used,
    MAX(vs."time") 	 				   as last_viewed,
    SUM(vs.nviews)                     as total_num_views

    FROM views_stats vs
    INNER JOIN views v
    	ON vs.view_id = v.id

    --- get viewer info ----
    INNER JOIN users u
    	ON  vs.user_id = u.id

    INNER JOIN system_users su
    	ON su.id = u.system_user_id

    GROUP BY 1
    ORDER BY 1
)

SELECT *
FROM WORKBOOK_SETUP ws
LEFT JOIN USED_PUBLISHED_DATASOURCES ud
	ON ws.wbk_id = ud.workbook_id

LEFT JOIN USED_FIELDS uf
	ON ws.wbk_id = uf.workbook_id

LEFT JOIN WORKBOOK_USAGE wu
	ON ws.wbk_id = wu.workbook_id


WHERE 1=1
    AND datasources_usedLIKE '%datasourcename%'
    OR wbk_owner IN ('username1', 'username2')
    OR wbk_name LIKE 'performance%'
