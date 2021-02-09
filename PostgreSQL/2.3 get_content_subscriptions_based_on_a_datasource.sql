
--check subscriptions for workbooks connected to a particular published datasource
WITH DATASOURCES as (
	SELECT
	v.id 	as view_id,
	v.name	as view_name,
	v.workbook_id,
	w.name 	as workbook_name,
	ds.name as datasource_name

	FROM public.workbooks w
	INNER JOIN views v
		ON v.workbook_id = w.id

	INNER JOIN public.datasources ds
		ON ds.parent_workbook_id = w.id

	INNER JOIN public.data_connections dc
		ON ds.id = dc.datasource_id

	WHERE 1=1
		AND dc.owner_type = 'Workbook'
		AND dc.dbclass = 'sqlproxy'
)

select
d.workbook_id,
d.workbook_name,
d.view_id,
d.view_name,
su.friendly_name as subscriber,
sub.subject,
sub.created_at,
sub.last_sent,
sub.data_condition_type,
sub.target_type,
sub.attach_pdf,
sub.attach_image

FROM public.subscriptions sub
LEFT join DATASOURCES d
	ON sub.target_id = d.workbook_id
		OR sub.target_id = d.view_id

-----get subscriber info------------
INNER JOIN users u
	ON sub.user_id = u.id

INNER JOIN system_users su
	ON su.id = u.system_user_id

WHERE 1=1
	AND datasource_name like '%DS%'

ORDER BY
workbook_id,
view_id
