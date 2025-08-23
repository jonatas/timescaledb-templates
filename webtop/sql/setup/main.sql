\i sql/maintenance/reset.sql
\i sql/setup/pipeline.sql
\i sql/setup/data-generation.sql

-- Configure for INTENSE 1GB/minute throughput (Presentation Demo)
-- Target: ~20M records/minute = ~1GB/minute
-- Retention: 5 minutes (for quick demo)
-- Elections: Every 5 minutes

-- Top websites: 100k hits/minute each (5 × 100k = 500k)
CALL schedule_top_websites_generation(100000);

-- Random websites: 1M hits/minute total
-- High traffic: 400k hits/minute
-- Medium traffic: 400k hits/minute  
-- Low traffic: 200k hits/minute
CALL schedule_random_websites_generation(400000);

-- Additional volume: 18.5M hits/minute total
-- This gives us ~20M records/minute total = ~1GB/minute
CALL schedule_additional_volume_generation(18500000);

CALL schedule_candidates_election();

-- Step 5: Final check of tables and jobs
\echo 'Setup complete. Checking final state...'
SELECT 'Website count:' AS check, COUNT(*) FROM top_websites_longterm;
SELECT 'Job status:' AS check;
SELECT job_id, proc_name, config, schedule_interval, next_start - now() AS next_start_in
FROM timescaledb_information.jobs
WHERE proc_name IN ('populate_website_candidates_job', 'elect_top_websites', 'generate_log_data_job');

\x
-- Check election system tables
SELECT * FROM top_websites_longterm LIMIT 1;
SELECT * FROM top_websites_report LIMIT 1;
