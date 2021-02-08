
WITH WBKS AS (
    --- which are my most used sheets within a workbook?
    --- when were they last viewed, how many times and by whom?
	SELECT
	prj.name		 as project,
	v.workbook_id,
	wb.name 		 as workbook_name,
	vs.view_id,
	v.name 			 as view_name,
	v.repository_url as view_url,
	sou.name		 as view_created_by,
	v.created_at     as view_created_at,
	v.index          as view_tab_num_in_wbk,
	v.title          as view_title,
	v.caption        as view_description,
	v.fields		 as fields_used_in_view,

	STRING_AGG(distinct
		vs.device_type, ', ')    as viewed_on,

	STRING_AGG(distinct
		su.friendly_name, ', ')  as viewed_by,

	MAX(vs."time")   as last_viewed,
	SUM(vs.nviews)   as total_num_views

	FROM views_stats vs
	INNER JOIN views v
	    ON vs.view_id = v.id

	--- get viewer info ----
	INNER JOIN users u
	    ON  vs.user_id = u.id

	INNER JOIN system_users su
	    ON su.id = u.system_user_id

	-----get owner info------------
	INNER JOIN users ou
	    ON  v.owner_id = ou.id

	INNER JOIN system_users sou
	    ON sou.id = ou.system_user_id

	 ----get workbook info ----------
	INNER JOIN public.workbooks wb
	 	ON v.workbook_id = wb.id

	 ------get project info---------
	INNER JOIN projects prj
		ON wb.project_id = prj.id

	GROUP BY 1,2,3,4,5,6,7,8,8,9,10,11,12

	ORDER BY
	workbook_id,
	view_id
)

, SUBSCRIPTIONS AS (
    --- who has subscribed to my workbook views?
	SELECT
	sub.target_id,
	sub.target_type,
	MAX(sub.last_sent)	as subscription_last_sent,

	STRING_AGG(distinct
		su.friendly_name, ', ')  as subscribers

	FROM public.subscriptions sub

	-----get subscriber info------------
	INNER JOIN users u
	    ON sub.user_id = u.id

	INNER JOIN system_users su
	    ON su.id = u.system_user_id

	GROUP BY 1,2
)

SELECT
w.project,
w.workbook_id,
w.workbook_name,
w.view_id,
w.view_name,
w.view_url,
w.view_created_by,
w.view_created_at,
w.view_tab_num_in_wbk,
w.view_title,
w.view_description,
w.fields_used_in_view,
w.viewed_on,
w.viewed_by,
w.last_viewed,
w.total_num_views,
s.subscription_last_sent,
s.subscribers,
s.target_type

FROM WBKS w
LEFT JOIN SUBSCRIPTIONS s
	ON w.view_id = s.target_id			--- capture view subscriptions
		OR w.workbook_id = s.target_id	--- capture workbook subscriptions


ORDER BY
workbook_id,
view_id
