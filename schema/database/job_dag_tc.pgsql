drop table if exists job_dag_tc;
create table job_dag_tc (
    job_id int references job(job_id),
    parent_job_id int references job(job_id)
);

create or replace function job_dag_tc_insert()
returns trigger language plpgsql
as $$
begin
	with tc_new as (
		-- add the new edge itself
		select new.job_id as job_id, new.parent_job_id as parent_job_id
		union
		-- connect the parents of the new parent to the new job
		select new.job_id as job_id, parent_job_id
		from job_dag_tc where job_id = new.parent_job_id
		union
		-- connect the children of the new job to the new parent
		select job_id, new.parent_job_id as parent_job_id
		from job_dag_tc where parent_job_id = new.job_id
		union
		-- connect all paths that were previously disconnected
		select a.job_id, b.parent_job_id
		from job_dag_tc a
		join job_dag_tc b on a.parent_job_id = new.job_id and b.job_id = new.parent_job_id
	)
	merge into job_dag_tc tc
	using tc_new on tc.job_id = tc_new.job_id and tc.parent_job_id = tc_new.parent_job_id
	when not matched then
	insert (job_id, parent_job_id) values (tc_new.job_id, tc_new.parent_job_id);
	return new;
end; $$;

create or replace trigger job_dag_insert_trigger before insert on job_dag
for each row execute function job_dag_tc_insert();

create or replace function job_dag_tc_delete()
returns trigger language plpgsql
as $$
begin
    with suspect as (
        select old.job_id as job_id, old.parent_job_id as parent_job_id
        union
        select b.job_id, a.parent_job_id
        from job_dag_tc a
        join job_dag_tc b on a.job_id = old.parent_job_id and b.parent_job_id = old.job_id
        union
        select old.job_id as job_id, parent_job_id
        from job_dag_tc
        where job_id = old.job_id
        union
        select job_id, old.parent_job_id as parent_job_id
        from job_dag_tc
        where parent_job_id = old.parent_job_id
    ), trusty as (
        select job_id, parent_job_id
        from job_dag_tc tc
        where not exists (
            select job_id, parent_job_id from suspect
            where suspect.parent_job_id = tc.parent_job_id and suspect.job_id = tc.job_id
        )
        union
        select job_id, parent_job_id
        from job_dag
        where parent_job_id <> old.parent_job_id and job_id <> old.job_id
    ), tc_new as (
        select job_id, parent_job_id
        from trusty
        union
        select t2.job_id, t1.parent_job_id
        from trusty t1
        join trusty t2 on t1.job_id = t2.parent_job_id
        union
        select t3.job_id, t1.parent_job_id
        from trusty t1
        join trusty t2 on t1.job_id = t2.parent_job_id
        join trusty t3 on t2.job_id = t3.parent_job_id
    )
    delete from job_dag_tc tc
	where not exists (select job_id, parent_job_id from tc_new where tc.job_id = tc_new.job_id and tc.parent_job_id = tc_new.parent_job_id)
	
	return old;
end; $$;

create or replace trigger job_dag_delete_trigger before delete on job_dag
for each row execute function job_dag_tc_delete();

create or replace function job_dag_tc_update()
returns trigger language plpgsql
as $$
begin
    raise exception 'update job_dag is not allowed';
    return new;
end; $$;

create or replace trigger job_dag_update_trigger before update on job_dag
for each statement execute function job_dag_tc_update();