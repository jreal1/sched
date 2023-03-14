drop table if exists job_dag_tc;
create table job_dag_tc (
    job_id int references job(job_id),
    parent_job_id int references job(job_id)
);