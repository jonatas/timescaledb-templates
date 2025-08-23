-- Pipeline to track top websites
-- Drop all existing objects
DROP TABLE IF EXISTS logs CASCADE;
DROP MATERIALIZED VIEW IF EXISTS website_stats_1m CASCADE;
DROP MATERIALIZED VIEW IF EXISTS website_stats_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS website_stats_1d CASCADE;
DROP TABLE IF EXISTS top_websites_config CASCADE;
DROP TABLE IF EXISTS top_websites_longterm CASCADE;
DROP TABLE IF EXISTS top_websites_candidates CASCADE;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;

-- Create base table
CREATE TABLE IF NOT EXISTS "logs" (
    "time" timestamp with time zone NOT NULL,
    "domain" text NOT NULL
);

-- Create hypertable with 20-minute chunks
SELECT create_hypertable('logs', 'time', 
    chunk_time_interval => INTERVAL '1 minute',
    if_not_exists => TRUE
);

-- Add retention policy to keep only last 5 minutes of logs (for demo)
SELECT add_retention_policy('logs', INTERVAL '5 minutes', schedule_interval => INTERVAL '1 minute');

-- Create hierarchical continuous aggregates
-- 1. 1-minute aggregation (base level from logs)
CREATE MATERIALIZED VIEW website_stats_1m
WITH (timescaledb.continuous, timescaledb.materialized_only=true) AS
    SELECT 
        time_bucket('1 minute', time) as bucket,
        domain,
        count(*) as total
    FROM logs
    GROUP BY 1,2
WITH NO DATA;

-- 2. 1-hour aggregation (from 1-minute)
CREATE MATERIALIZED VIEW website_stats_1h
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
    SELECT 
        time_bucket('1 hour', bucket) as bucket,
        domain,
        sum(total) as total,
        stats_agg(total) as stats_agg,
        percentile_agg(total) as percentile_agg
    FROM website_stats_1m
    GROUP BY 1,2
WITH NO DATA;

-- 3. 1-day aggregation (from 1-hour)
CREATE MATERIALIZED VIEW website_stats_1d
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
    SELECT 
        time_bucket('1 day', bucket) as bucket,
        domain,
        sum(total) as total,
        stats_agg(total) as stats_agg,
        percentile_agg(total) as percentile_agg
    FROM website_stats_1h
    GROUP BY 1,2
WITH NO DATA;

-- Set refresh policies for each level
-- 1-minute level: refresh every 30 seconds, keep last 5 minutes
SELECT add_continuous_aggregate_policy('website_stats_1m',
    start_offset => INTERVAL '5 minutes',
    end_offset => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds');

-- 1-hour level: refresh every 5 minutes, keep last 30 days
SELECT add_continuous_aggregate_policy('website_stats_1h',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');

-- 1-day level: refresh every 1 hour, keep last 90 days
SELECT add_continuous_aggregate_policy('website_stats_1d',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Initial refresh of all aggregates
CALL refresh_continuous_aggregate('website_stats_1m', NULL, NULL);
CALL refresh_continuous_aggregate('website_stats_1h', NULL, NULL);
CALL refresh_continuous_aggregate('website_stats_1d', NULL, NULL);

-- Configuration table for top websites tracking
CREATE TABLE IF NOT EXISTS "top_websites_config" (
    "id" serial PRIMARY KEY,
    "min_hits_threshold" integer NOT NULL DEFAULT 100,
    "election_window" interval NOT NULL DEFAULT '1 hour',
    "longterm_retention_period" interval NOT NULL DEFAULT '90 days',
    "candidates_retention_period" interval NOT NULL DEFAULT '7 days',
    "max_longterm_entries" integer NOT NULL DEFAULT 1000,
    "election_schedule_interval" interval NOT NULL DEFAULT '1 hour',
    "created_at" timestamp with time zone NOT NULL DEFAULT now(),
    "updated_at" timestamp with time zone NOT NULL DEFAULT now()
);

-- Insert default configuration if none exists
INSERT INTO "top_websites_config" 
    ("min_hits_threshold", "election_window", "longterm_retention_period", "candidates_retention_period", 
     "max_longterm_entries", "election_schedule_interval")
SELECT 100, '5 minutes', '90 days', '7 days', 1000, '5 minutes'
WHERE NOT EXISTS (SELECT 1 FROM top_websites_config);

-- Long-term top websites storage
CREATE TABLE IF NOT EXISTS "top_websites_longterm" (
    "domain" text NOT NULL,
    "first_seen" timestamp with time zone NOT NULL,
    "last_seen" timestamp with time zone NOT NULL,
    "total_hits" bigint NOT NULL DEFAULT 0,
    "last_election_hits" bigint NOT NULL DEFAULT 0,
    "times_elected" integer NOT NULL DEFAULT 1,
    PRIMARY KEY ("domain")
) WITH (
    fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_analyze_scale_factor = 0.025,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_threshold = 50
);

-- Candidates table
CREATE TABLE IF NOT EXISTS "top_websites_candidates" (
    "domain" text NOT NULL,
    "time" timestamp with time zone NOT NULL,
    "hits" bigint NOT NULL,
    "created_at" timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY ("domain", "time")
);

SELECT create_hypertable('top_websites_candidates', 'time', if_not_exists => TRUE);


-- Create the election procedure
CREATE OR REPLACE PROCEDURE elect_top_websites(job_id INTEGER, config JSONB)
LANGUAGE plpgsql AS $$
DECLARE
    max_entries INTEGER;
    retention_period INTERVAL;
BEGIN
    -- Get configuration
    SELECT max_longterm_entries, longterm_retention_period 
    INTO max_entries, retention_period
    FROM top_websites_config 
    LIMIT 1;
    
    -- Upsert top websites using ON CONFLICT
    INSERT INTO top_websites_longterm (
        domain,
        first_seen,
        last_seen,
        total_hits,
        last_election_hits,
        times_elected
    )
    SELECT 
        domain,
        time AS first_seen,
        time AS last_seen,
        hits AS total_hits,
        hits AS last_election_hits,
        1 AS times_elected
    FROM top_websites_candidates tc
    WHERE tc.time = (SELECT MAX(time) FROM top_websites_candidates WHERE domain = tc.domain)
    ON CONFLICT (domain) DO UPDATE SET
        last_seen = EXCLUDED.last_seen,
        total_hits = top_websites_longterm.total_hits + EXCLUDED.total_hits,
        last_election_hits = EXCLUDED.last_election_hits,
        times_elected = top_websites_longterm.times_elected + 1;

    -- Enforce max_longterm_entries by deleting oldest entries if limit exceeded
    DELETE FROM top_websites_longterm
    WHERE domain IN (
        SELECT domain
        FROM top_websites_longterm
        ORDER BY last_seen ASC
        OFFSET max_entries
    );

    -- Clean up old candidates
    DELETE FROM top_websites_candidates
    WHERE time < now() - retention_period;

    RAISE NOTICE 'Election complete. % websites in long-term storage.', (SELECT COUNT(*) FROM top_websites_longterm);
END;
$$;

-- Create the job to populate website candidates
CREATE OR REPLACE PROCEDURE populate_website_candidates_job(job_id INTEGER, config JSONB)
LANGUAGE plpgsql AS $$
DECLARE
    min_hits INTEGER;
    v_election_window INTERVAL;
    candidate_count INTEGER;
BEGIN
    -- Get configuration
    SELECT min_hits_threshold, election_window
    INTO min_hits, v_election_window
    FROM top_websites_config
    LIMIT 1;

    -- Populate candidates from website_stats_1m for the last election_window
    INSERT INTO top_websites_candidates (domain, time, hits)
    SELECT 
        domain,
        MAX(bucket) AS time,
        SUM(total) AS hits
    FROM website_stats_1m
    WHERE bucket >= NOW() - INTERVAL '30 minutes'
    GROUP BY domain
    HAVING SUM(total) >= 100
    ON CONFLICT (domain, time) DO UPDATE SET hits = EXCLUDED.hits;

    GET DIAGNOSTICS candidate_count = ROW_COUNT;
    RAISE NOTICE 'Populated % website candidates.', candidate_count;
END;
$$;

-- Function to schedule website tracking jobs
CREATE OR REPLACE PROCEDURE schedule_candidates_election()
LANGUAGE plpgsql AS $$
DECLARE
    election_schedule INTERVAL;
    config_record RECORD;
BEGIN
    -- Get configuration for scheduling
    SELECT * INTO config_record FROM top_websites_config LIMIT 1;

    PERFORM add_job('populate_website_candidates_job',
        (config_record.election_schedule_interval / 10)::interval,
        initial_start => NOW() + INTERVAL '10 seconds'
    );

    PERFORM add_job('elect_top_websites',
        config_record.election_schedule_interval,
        initial_start => NOW() + INTERVAL '1 minute'
    );
END;
$$;

-- Create a view to report top websites
CREATE OR REPLACE VIEW top_websites_report AS
SELECT 
    domain,
    total_hits,
    last_election_hits,
    times_elected,
    first_seen,
    last_seen,
    CASE 
        WHEN last_seen = first_seen THEN 0
        ELSE ROUND((total_hits::numeric / EXTRACT(EPOCH FROM (last_seen - first_seen)) * 3600), 2)
    END AS avg_hourly_requests
FROM top_websites_longterm
ORDER BY total_hits DESC;

