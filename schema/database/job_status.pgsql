drop table if exists job_status;
create table job_status(
    "timestamp" timestamp,
    job_id int references job(job_id),
    status varchar(25),
    message varchar
);