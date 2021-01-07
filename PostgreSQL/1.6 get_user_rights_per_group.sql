
SELECT
s.name                  as site_name,
g.name                  as group_name,
sysus.name              as user_id,
sysus.friendly_name     as user_name,
r.id                    as role_id,
r.name                  as role_name,
string_agg(c.name, ', '
	order by c.name)	as capabilities

FROM public.group_users usg
INNER JOIN public.groups g
	ON usg.group_id = g.id

-------get user details ----------
INNER JOIN public.users u
	ON usg.user_id = u.id

INNER JOIN public.system_users sysus
	ON sysus.id = u.system_user_id

--------get role details ----------
INNER JOIN public.roles r
	ON r.id = u.site_role_id

INNER JOIN public.capability_roles cr
	ON cr.role_id = r.id

INNER JOIN public.capabilities c
	ON cr.capability_id = c.id

------get site details---------------
INNER JOIN sites s
	ON g.site_id = s.id

WHERE g.site_id = 1 --Default site
	AND usg.group_id NOT IN (2) --all users

GROUP BY 1,2,3,4,5,6

ORDER BY 1,2
