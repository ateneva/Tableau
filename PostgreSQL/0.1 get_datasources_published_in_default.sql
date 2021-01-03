SELECT
ds.id,
ds.name,
ds.created_at,
ds.updated_at,
ds.size,
ds.state,
ds.db_name,
ds.table_name,
ds.site_id,
ds.revision,
ds.embedded,
ds.incrementable_extracts,		--incremental refresh is allowed
ds.refreshable_extracts,		--full refresh is allowed
ds.extracts_refreshed_at,		--last full refresh
ds.extracts_incremented_at,		--last incremental refresh
ds.description,					--datsource About Note
ds.content_version,				--content version
ds.first_published_at,			--dataseource was first published on
ds.last_published_at,			--datasouece was last published on
ds.is_certified,				--datasource has been certified
ds.certifier_details,			--datasource has been certified by
ds.certification_note,
pr.name as project

FROM public.datasources ds
INNER JOIN public.projects pr on ds.project_id = pr.id

WHERE project_id = 1 --Default project
 	AND repository_url NOT LIKE '%embedded%'
