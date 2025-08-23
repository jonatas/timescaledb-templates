-- Data Generation Functions for Top Websites Tracker
-- This file contains all functions related to generating synthetic data

-- Drop existing functions first
DROP PROCEDURE IF EXISTS generate_log_data(text, integer);
DROP PROCEDURE IF EXISTS schedule_data_generation();
DROP PROCEDURE IF EXISTS schedule_top_websites_generation();
DROP PROCEDURE IF EXISTS schedule_random_websites_generation();
DROP PROCEDURE IF EXISTS schedule_random_websites_generation(int);
DROP PROCEDURE IF EXISTS schedule_additional_volume_generation(int);
DROP PROCEDURE IF EXISTS generate_log_data_job(int, jsonb);

-- Procedure to generate synthetic log data every second
CREATE OR REPLACE PROCEDURE generate_log_data_job(job_id_param int, config_param jsonb) 
LANGUAGE plpgsql AS $$
DECLARE
    amount INTEGER;
    domain TEXT;
    use_specific_domain BOOLEAN;
BEGIN
    -- Get configuration
    amount := (config_param->>'amount')::INTEGER;
    domain := config_param->>'domain';
    use_specific_domain := COALESCE((config_param->>'use_specific_domain')::BOOLEAN, false);
    
    -- Generate logs
    IF use_specific_domain THEN
        -- Generate logs for specific domain
        INSERT INTO logs (time, domain)
        SELECT 
            now() - (random() * interval '1 minute'),
            domain
        FROM generate_series(1, amount);
        
        RAISE NOTICE 'Generated % log entries for % at %', amount, domain, now();
    ELSE
        -- Generate logs for random domains
        INSERT INTO logs (time, domain)
        SELECT 
            now() - (random() * interval '1 minute'),
            'random-domain-' || floor(random() * 1000)::text
        FROM generate_series(1, amount);
        
        RAISE NOTICE 'Generated % random log entries at %', amount, now();
    END IF;
END;
$$;

-- Function to generate test data manually
CREATE OR REPLACE PROCEDURE generate_log_data(
    domain_name text DEFAULT NULL,
    amount integer DEFAULT 100
) 
LANGUAGE plpgsql AS $$
DECLARE
    config jsonb;
BEGIN
    IF domain_name IS NULL THEN
        -- Generate random domain data
        config := jsonb_build_object(
            'use_specific_domain', false,
            'amount', amount
        );
    ELSE
        -- Generate specific domain data
        config := jsonb_build_object(
            'use_specific_domain', true,
            'domain', domain_name,
            'amount', amount
        );
    END IF;
    
    PERFORM generate_log_data_job(0, config);
END;
$$;

-- Function to schedule data generation
CREATE OR REPLACE PROCEDURE schedule_data_generation(amount int DEFAULT 100) 
LANGUAGE plpgsql AS $$
BEGIN
    -- Schedule random data generation (runs every minute)
    PERFORM add_job(
        'generate_log_data_job',
        INTERVAL '1 minute',
        config => jsonb_build_object(
            'amount', amount,
            'use_specific_domain', false
        ),
        initial_start => NOW() + INTERVAL '1 minute'
    );
END;
$$;

-- Function to schedule data generation for top websites
CREATE OR REPLACE PROCEDURE schedule_top_websites_generation(avg_hits_per_minute int DEFAULT 30) 
LANGUAGE plpgsql AS $$
DECLARE
    top_websites text[] := ARRAY['google.com', 'facebook.com', 'youtube.com', 'twitter.com', 'instagram.com'];
    website text;
    amount int;
BEGIN
    FOREACH website IN ARRAY top_websites LOOP
        amount := avg_hits_per_minute + (random() * avg_hits_per_minute * 0.2)::integer;
        PERFORM add_job(
            'generate_log_data_job', 
            INTERVAL '5 seconds', 
            config => jsonb_build_object(
                'use_specific_domain', true,
                'domain', website,
                'amount', amount
            )
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE PROCEDURE schedule_random_websites_generation(avg_hits_per_minute int DEFAULT 30) 
LANGUAGE plpgsql AS $$
DECLARE
    amount int;
BEGIN
    amount := avg_hits_per_minute + (random() * avg_hits_per_minute * 0.2)::integer;
    PERFORM add_job(
        'generate_log_data_job', 
        INTERVAL '30 seconds', 
        config => jsonb_build_object(
            'use_specific_domain', false,
            'domain_min', 400,
            'domain_max', 1000,
            'amount', amount
        )
    );
    
    -- Medium-traffic random domains (10-50 hits every 15 seconds)
    amount := avg_hits_per_minute + (random() * avg_hits_per_minute * 0.2)::integer;
    PERFORM add_job(
        'generate_log_data_job', 
        INTERVAL '15 seconds', 
        config => jsonb_build_object(
            'use_specific_domain', false,
            'domain_min', 100,
            'domain_max', 399,
            'amount', amount
        )
    );
    
    -- High-traffic random domains (50-100 hits every 10 seconds)
    amount := avg_hits_per_minute + (random() * avg_hits_per_minute * 0.2)::integer;
    PERFORM add_job(
        'generate_log_data_job', 
        INTERVAL '10 seconds', 
        config => jsonb_build_object(
            'use_specific_domain', false,
            'domain_min', 10,
            'domain_max', 99,
            'amount', amount
        )
    );
END;
$$; 

CREATE OR REPLACE PROCEDURE schedule_additional_volume_generation(avg_hits_per_minute int DEFAULT 1000000) 
LANGUAGE plpgsql AS $$
DECLARE
    amount int;
    i int;
BEGIN
    -- Create multiple high-volume jobs to distribute the load
    -- Each job generates a portion of the total volume
    
    -- Job 1: High-volume random domains (every 2 seconds)
    amount := (avg_hits_per_minute / 30) + (random() * avg_hits_per_minute * 0.1)::integer;
    PERFORM add_job(
        'generate_log_data_job', 
        INTERVAL '2 seconds', 
        config => jsonb_build_object(
            'use_specific_domain', false,
            'domain_min', 1,
            'domain_max', 9999,
            'amount', amount
        )
    );
    
    -- Job 2: Medium-volume random domains (every 3 seconds)
    amount := (avg_hits_per_minute / 20) + (random() * avg_hits_per_minute * 0.1)::integer;
    PERFORM add_job(
        'generate_log_data_job', 
        INTERVAL '3 seconds', 
        config => jsonb_build_object(
            'use_specific_domain', false,
            'domain_min', 10000,
            'domain_max', 19999,
            'amount', amount
        )
    );
    
    -- Job 3: Low-volume random domains (every 5 seconds)
    amount := (avg_hits_per_minute / 12) + (random() * avg_hits_per_minute * 0.1)::integer;
    PERFORM add_job(
        'generate_log_data_job', 
        INTERVAL '5 seconds', 
        config => jsonb_build_object(
            'use_specific_domain', false,
            'domain_min', 20000,
            'domain_max', 29999,
            'amount', amount
        )
    );
    
    -- Job 4: Burst traffic simulation (every 10 seconds)
    amount := (avg_hits_per_minute / 6) + (random() * avg_hits_per_minute * 0.2)::integer;
    PERFORM add_job(
        'generate_log_data_job', 
        INTERVAL '10 seconds', 
        config => jsonb_build_object(
            'use_specific_domain', false,
            'domain_min', 30000,
            'domain_max', 39999,
            'amount', amount
        )
    );
    
    RAISE NOTICE 'Scheduled additional volume generation: % hits/minute across 4 jobs', avg_hits_per_minute;
END;
$$; 