
SELECT
usg.group_id            as GroupID,
g.name                  as GroupPermissions,
sysus.friendly_name     as UserName,
sysus.email             as UserEmail

FROM public.group_users usg
INNER JOIN public.groups g
	ON usg.group_id = g.id

INNER JOIN public.users u
	ON usg.user_id = u.id

INNER JOIN public.system_users sysus
	ON sysus.id = u.system_user_id

WHERE g.site_id = 1 --Default site
	AND usg.group_id NOT IN (2) --all users

ORDER BY 1
