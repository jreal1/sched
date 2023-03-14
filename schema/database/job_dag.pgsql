drop table if exists job_dag;
create table job_dag (
    job_id int references job(job_id),
    parent_job_id int references job(job_id)
);