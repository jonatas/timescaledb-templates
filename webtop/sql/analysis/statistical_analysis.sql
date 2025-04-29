-- WebTop - Statistical Analysis Functions
-- This file adds advanced statistical analysis capabilities using TimescaleDB Toolkit

-- Create a view for advanced statistical analysis of domain traffic
CREATE OR REPLACE VIEW domain_traffic_stats AS
SELECT 
    time_bucket('1 hour', time) AS bucket,
    domain,
    stats_agg(1) AS request_stats,  -- Aggregate for request counts
    percentile_agg(1) AS request_percentiles  -- Percentile aggregation for request counts
FROM logs
GROUP BY 1, 2;

-- Create a continuous aggregate for domain traffic statistics
CREATE MATERIALIZED VIEW domain_traffic_stats_1h
WITH (timescaledb.continuous, timescaledb.materialized_only=false) AS
SELECT 
    time_bucket('1 hour', time) AS bucket,
    domain,
    stats_agg(1) AS request_stats,
    percentile_agg(1) AS request_percentiles
FROM logs
GROUP BY 1, 2
WITH NO DATA;

-- Set refresh policy for the continuous aggregate
SELECT add_continuous_aggregate_policy('domain_traffic_stats_1h',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '5 minutes',
    schedule_interval => INTERVAL '5 minutes');

-- Create a view for traffic pattern analysis
CREATE OR REPLACE VIEW traffic_patterns_monitor AS
SELECT 
    domain,
    SUM(num_vals(request_stats)) AS total_requests,
    ROUND(avg(request_stats)::numeric, 2) AS avg_requests_per_minute,
    ROUND(stddev(request_stats)::numeric, 2) AS stddev_requests,
    ROUND(skewness(request_stats)::numeric, 2) AS skewness,
    ROUND(kurtosis(request_stats)::numeric, 2) AS kurtosis,
    CASE 
        WHEN avg(request_stats) > percentile_agg_approx(0.75, request_percentiles) THEN 'High'
        WHEN avg(request_stats) > percentile_agg_approx(0.25, request_percentiles) THEN 'Medium'
        ELSE 'Low'
    END AS current_traffic_level,
    CASE 
        WHEN avg(request_stats) > LAG(avg(request_stats)) OVER (PARTITION BY domain ORDER BY bucket) THEN 'Growing'
        WHEN avg(request_stats) < LAG(avg(request_stats)) OVER (PARTITION BY domain ORDER BY bucket) THEN 'Declining'
        ELSE 'Stable'
    END AS trend
FROM domain_traffic_stats_1h
WHERE bucket > NOW() - INTERVAL '1 hour'
GROUP BY domain
ORDER BY total_requests DESC;

-- Function to analyze traffic patterns with advanced statistics
CREATE OR REPLACE FUNCTION analyze_traffic_patterns(
    domain_name TEXT DEFAULT NULL,
    time_window INTERVAL DEFAULT '1 hour'
)
RETURNS TABLE (
    domain TEXT,
    time_bucket TIMESTAMPTZ,
    total_requests BIGINT,
    avg_requests DOUBLE PRECISION,
    stddev_requests DOUBLE PRECISION,
    p25_requests DOUBLE PRECISION,
    p50_requests DOUBLE PRECISION,
    p75_requests DOUBLE PRECISION,
    p95_requests DOUBLE PRECISION,
    skewness DOUBLE PRECISION,
    kurtosis DOUBLE PRECISION,
    traffic_level TEXT,
    trend TEXT
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        d.domain,
        d.bucket AS time_bucket,
        SUM(num_vals(d.request_stats))::BIGINT AS total_requests,
        ROUND(avg(d.request_stats)::numeric, 2) AS avg_requests,
        ROUND(stddev(d.request_stats)::numeric, 2) AS stddev_requests,
        ROUND(percentile_agg_approx(0.25, d.request_percentiles)::numeric, 2) AS p25_requests,
        ROUND(percentile_agg_approx(0.50, d.request_percentiles)::numeric, 2) AS p50_requests,
        ROUND(percentile_agg_approx(0.75, d.request_percentiles)::numeric, 2) AS p75_requests,
        ROUND(percentile_agg_approx(0.95, d.request_percentiles)::numeric, 2) AS p95_requests,
        ROUND(skewness(d.request_stats)::numeric, 2) AS skewness,
        ROUND(kurtosis(d.request_stats)::numeric, 2) AS kurtosis,
        CASE 
            WHEN avg(d.request_stats) > percentile_agg_approx(0.75, d.request_percentiles) THEN 'High'
            WHEN avg(d.request_stats) > percentile_agg_approx(0.25, d.request_percentiles) THEN 'Medium'
            ELSE 'Low'
        END AS traffic_level,
        CASE 
            WHEN avg(d.request_stats) > LAG(avg(d.request_stats)) OVER (PARTITION BY d.domain ORDER BY d.bucket) THEN 'Growing'
            WHEN avg(d.request_stats) < LAG(avg(d.request_stats)) OVER (PARTITION BY d.domain ORDER BY d.bucket) THEN 'Declining'
            ELSE 'Stable'
        END AS trend
    FROM domain_traffic_stats_1h d
    WHERE d.bucket > NOW() - time_window
    AND (domain_name IS NULL OR d.domain = domain_name)
    GROUP BY d.domain, d.bucket
    ORDER BY d.domain, d.bucket DESC;
END;
$$;

-- Function to detect traffic anomalies using statistical methods
CREATE OR REPLACE FUNCTION detect_traffic_anomalies(
    domain_name TEXT DEFAULT NULL,
    time_window INTERVAL DEFAULT '1 hour',
    threshold DOUBLE PRECISION DEFAULT 2.0
)
RETURNS TABLE (
    domain TEXT,
    time_bucket TIMESTAMPTZ,
    actual_requests BIGINT,
    expected_requests DOUBLE PRECISION,
    deviation DOUBLE PRECISION,
    is_anomaly BOOLEAN
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH traffic_stats AS (
        SELECT 
            d.domain,
            d.bucket,
            SUM(num_vals(d.request_stats))::BIGINT AS total_requests,
            avg(d.request_stats) AS avg_requests,
            stddev(d.request_stats) AS stddev_requests
        FROM domain_traffic_stats_1h d
        WHERE d.bucket > NOW() - time_window
        AND (domain_name IS NULL OR d.domain = domain_name)
        GROUP BY d.domain, d.bucket
    )
    SELECT 
        ts.domain,
        ts.bucket AS time_bucket,
        ts.total_requests AS actual_requests,
        ROUND(ts.avg_requests::numeric, 2) AS expected_requests,
        ROUND(
            CASE 
                WHEN ts.stddev_requests = 0 THEN 0
                ELSE ABS(ts.total_requests - ts.avg_requests) / ts.stddev_requests
            END::numeric, 
            2
        ) AS deviation,
        CASE 
            WHEN ts.stddev_requests = 0 THEN FALSE
            ELSE ABS(ts.total_requests - ts.avg_requests) > (ts.stddev_requests * threshold)
        END AS is_anomaly
    FROM traffic_stats ts
    ORDER BY ts.domain, ts.bucket DESC;
END;
$$;

-- Function to generate a traffic report with advanced statistics
CREATE OR REPLACE FUNCTION generate_traffic_report(
    time_window INTERVAL DEFAULT '1 day'
)
RETURNS TABLE (
    domain TEXT,
    total_requests BIGINT,
    avg_requests_per_hour DOUBLE PRECISION,
    stddev_requests DOUBLE PRECISION,
    p95_requests DOUBLE PRECISION,
    max_requests BIGINT,
    traffic_volatility DOUBLE PRECISION,
    traffic_trend TEXT,
    anomaly_count BIGINT
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    WITH domain_stats AS (
        SELECT 
            d.domain,
            SUM(num_vals(d.request_stats))::BIGINT AS total_requests,
            ROUND(avg(d.request_stats)::numeric, 2) AS avg_requests,
            ROUND(stddev(d.request_stats)::numeric, 2) AS stddev_requests,
            ROUND(percentile_agg_approx(0.95, d.request_percentiles)::numeric, 2) AS p95_requests,
            MAX(num_vals(d.request_stats))::BIGINT AS max_requests,
            ROUND(
                CASE 
                    WHEN avg(d.request_stats) = 0 THEN 0
                    ELSE stddev(d.request_stats) / avg(d.request_stats)
                END::numeric, 
                2
            ) AS traffic_volatility,
            CASE 
                WHEN avg(d.request_stats) > LAG(avg(d.request_stats)) OVER (PARTITION BY d.domain ORDER BY d.bucket) THEN 'Growing'
                WHEN avg(d.request_stats) < LAG(avg(d.request_stats)) OVER (PARTITION BY d.domain ORDER BY d.bucket) THEN 'Declining'
                ELSE 'Stable'
            END AS traffic_trend
        FROM domain_traffic_stats_1h d
        WHERE d.bucket > NOW() - time_window
        GROUP BY d.domain
    ),
    anomaly_counts AS (
        SELECT 
            domain,
            COUNT(*) AS anomaly_count
        FROM detect_traffic_anomalies(NULL, time_window)
        WHERE is_anomaly = TRUE
        GROUP BY domain
    )
    SELECT 
        ds.domain,
        ds.total_requests,
        ds.avg_requests AS avg_requests_per_hour,
        ds.stddev_requests,
        ds.p95_requests,
        ds.max_requests,
        ds.traffic_volatility,
        ds.traffic_trend,
        COALESCE(ac.anomaly_count, 0) AS anomaly_count
    FROM domain_stats ds
    LEFT JOIN anomaly_counts ac ON ds.domain = ac.domain
    ORDER BY ds.total_requests DESC;
END;
$$; 