drop table if exists job;
create table job (
    job_id int primary key generated always as identity,
    job_type_id int references job_type(job_type_id),
    job_name varchar(255)
);