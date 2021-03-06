SELECT
  job.id                      as job_id,
  job.progress                as progress,
  CAST(job.args as TEXT)      as args,
  CAST(job.notes as TEXT)     as notes,
  job.updated_at              as updated_at,
  job.created_at              as created_at,
  job.completed_at            as completed_at,
  job.started_at              as started_at,
  job.job_name                as job_name,
  job.finish_code             as finish_code,
  job.priority                as priority,
  job.title                   as title,
  job.created_on_worker       as created_on_worker,
  job.processed_on_worker     as processed_on_worker,
  CAST(job.link as TEXT)      as link,
  job.lock_version            as lock_version,
  job.backgrounder_id         as backgrounder_id,
  job.serial_collection_id    as serial_collection_id,
  job.site_id                 as site_id,
  job.subtitle                as subtitle,
  job.language                as language,
  job.locale                  as locale,
  job.correlation_id          as correlation_id,
  job.attempts_remaining      as attempts_remaining,
  job.luid                    as luid,
  job.job_rank                as job_rank,
  job.queue_id                as queue_id,
  job.overflow                as overflow,
  job.promoted_at             as promoted_at,
  job.task_id                 as task_id,
  job.run_now                 as run_now,
  job.creator_id              as creator_id,
  s.id                        as site_id,
  s.name                      as site_name,
  s.url_namespace             as url_namespace,
  s.status                    as status

FROM public.background_jobs job
INNER JOIN public.sites s
    ON (job.site_id = s.id)

WHERE job.job_name LIKE 'Refresh%'
    OR job.job_name LIKE 'Subscription%'
