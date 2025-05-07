-- Hourly anomaly summary (15-min refresh)
CREATE MATERIALIZED VIEW anomaly_rollup
DISTRIBUTED BY HASH(device_id)
REFRESH ASYNC EVERY (INTERVAL 15 MINUTE)
AS
SELECT 
    device_id,
    DATE_TRUNC('hour', timestamp) AS hour_bucket,
    COUNT(*) AS total_anomalies,
    COUNT_IF(is_confirmed = false) AS unconfirmed_count,
    COUNT_IF(severity = 'high') AS high_severity_count,
    ARRAY_AGG(DISTINCT anomaly_type) AS anomaly_types
FROM 
    anomalies_streaming
GROUP BY 
    1, 2;



-- Daily refreshed view combining batch and streaming
CREATE MATERIALIZED VIEW unified_device_health
REFRESH ASYNC EVERY (INTERVAL 1 HOUR)
AS
SELECT 
    b.device_id,
    b.date,
    -- Available batch metrics from gold_device_health
    b.min_health_score AS health_score,
    b.days_since_maintenance,
    -- Streaming metrics
    COALESCE(s.unconfirmed_count, 0) AS pending_anomalies,
    COALESCE(s.high_severity_count, 0) AS urgent_anomalies,
    -- Revised combined score using available columns
    ROUND(
        ((1 - b.min_health_score) * 0.6) +  -- Inverse since lower health_score = worse
        (LEAST(s.unconfirmed_count, 10)/10.0 * 0.3) +
        (CASE WHEN b.days_since_maintenance > 90 THEN 0.1 ELSE 0 END),
    2) AS criticality_score,
    -- Derived priority based on existing data
    CASE
        WHEN (1 - b.min_health_score) > 0.7 OR b.days_since_maintenance > 120 THEN 'P0'
        WHEN (1 - b.min_health_score) > 0.5 OR b.days_since_maintenance > 90 THEN 'P1'
        WHEN b.vibration_anomalies > 5 OR b.temp_anomalies > 3 THEN 'P2'
        ELSE 'P3'
    END AS priority
FROM 
    iceberg_catalog_iot_gold.iot.gold_device_health b
LEFT JOIN (
    SELECT 
        device_id,
        DATE_TRUNC('day', hour_bucket) AS date,
        SUM(unconfirmed_count) AS unconfirmed_count,
        SUM(high_severity_count) AS high_severity_count
    FROM 
        anomaly_rollup
    GROUP BY 
        1, 2
) s ON b.device_id = s.device_id AND b.date = s.date;


-- Real-time Anomaly Status
SELECT 
    a.device_id,
    h.priority,
    COUNT(a.anomaly_type) AS new_anomalies,
    ARRAY_JOIN(ARRAY_AGG(DISTINCT a.anomaly_type), ', ') AS anomaly_types
FROM 
    anomalies_streaming a
JOIN 
    unified_device_health h ON a.device_id = h.device_id
WHERE 
    a.timestamp >= NOW() - INTERVAL '1 HOUR'
    AND h.date = CURRENT_DATE()
GROUP BY 
    1, 2
ORDER BY 
    h.priority, 
    COUNT(a.anomaly_type) DESC
LIMIT 20;