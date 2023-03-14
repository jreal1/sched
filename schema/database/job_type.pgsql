drop table if exists job_type;
create table job_type (
    job_type_id int primary key generated always as identity,
    job_type_name varchar(255)
);