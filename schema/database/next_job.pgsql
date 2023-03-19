drop view if exists next_job;
create view next_job as
with hist as (
	select max(coalesce(timestamp, '0001-01-01 00:00:00.000')) last_success, j.job_id
	from job j
	left join job_status js on j.job_id = js.job_id
	where status='Success' or status is null
	group by j.job_id
)
select dag.job_id next_job_id
from hist ph
join job_dag dag on ph.job_id=dag.parent_job_id
join hist h on h.job_id=dag.job_id
group by dag.job_id
having min(ph.last_success) > max(h.last_success)
;

