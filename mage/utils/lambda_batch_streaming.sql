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
    b.min_health_score AS health_score,
    b.days_since_maintenance,
    
    -- Streaming metrics directly from anomalies_streaming
    COUNT_IF(s.is_confirmed = FALSE) AS pending_anomalies,
    COUNT_IF(s.is_confirmed = FALSE AND s.severity = 'high') AS urgent_anomalies,

    -- Criticality Score
    ROUND(
        ((1 - b.min_health_score) * 0.6) +
        (LEAST(COUNT_IF(s.is_confirmed = FALSE), 10)/10.0 * 0.3) +
        (CASE WHEN b.days_since_maintenance > 90 THEN 0.1 ELSE 0 END),
    2) AS criticality_score,

    -- Priority logic
    CASE
        WHEN (1 - b.min_health_score) > 0.7 OR b.days_since_maintenance > 120 THEN 'P0'
        WHEN (1 - b.min_health_score) > 0.5 OR b.days_since_maintenance > 90 THEN 'P1'
        WHEN b.vibration_anomalies > 5 OR b.temp_anomalies > 3 THEN 'P2'
        ELSE 'P3'
    END AS priority

FROM 
    iceberg_catalog_iot_gold.iot.gold_device_health b
LEFT JOIN 
    anomalies_streaming s
    ON b.device_id = s.device_id 
    AND DATE_TRUNC('day', s.timestamp) = b.date

GROUP BY 
    b.device_id,
    b.date,
    b.min_health_score,
    b.days_since_maintenance,
    b.vibration_anomalies,
    b.temp_anomalies;
