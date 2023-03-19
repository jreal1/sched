truncate table job_dag, job_dag_tc, job_type, job cascade; --also truncates job table
insert into job_type (job_type_id, job_type_name) OVERRIDING SYSTEM VALUE values (1, 'pyjob');
insert into job (job_id, job_type_id, job_name) OVERRIDING SYSTEM VALUE values (1, 1, 'A');
insert into job (job_id, job_type_id, job_name) OVERRIDING SYSTEM VALUE values (2, 1, 'B');
insert into job (job_id, job_type_id, job_name) OVERRIDING SYSTEM VALUE values (3, 1, 'C');
insert into job_dag (job_id, parent_job_id) values (2,1),(3,2);
insert into job_dag_tc(job_id, parent_job_id) values (2,1),(3,1),(3,2)

