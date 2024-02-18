--Adjust parallel settings using psql
ALTER SYSTEM SET max_parallel_workers = 6;
ALTER SYSTEM SET max_parallel_workers_per_gather = 3;
ALTER SYSTEM SET parallel_setup_cost = '50';
ALTER SYSTEM SET parallel_tuple_cost = '0.05';
SELECT pg_reload_conf();