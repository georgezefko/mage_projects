/*

STREAMING QUERIES

*/

-- Updated anomalies_streaming table
CREATE TABLE anomalies_streaming (
    device_id STRING,
    timestamp DATETIME,
    anomaly_type STRING,
    severity STRING,
    description STRING,
    is_confirmed BOOLEAN,
    confirmed_by STRING NULL
    
    )
PRIMARY KEY (device_id, timestamp)
PARTITION BY date_trunc('day', timestamp)
DISTRIBUTED BY HASH(device_id)
ORDER BY (timestamp, severity)
PROPERTIES (
    "enable_persistent_index" = "true",
    "replication_num" = "1"
);


-- ROUTINE LOAD TO INSERT DATA FROM KAFKA TO STARROCKS
CREATE ROUTINE LOAD iot_anomalies_load ON anomalies_streaming
COLUMNS (device_id, timestamp, anomaly_type, severity, description, is_confirmed, confirmed_by)
PROPERTIES (
    "desired_concurrent_number" = "1",
    "format" = "json",
    "jsonpaths" = "[\"$[*].device_id\", \"$[*].timestamp\", \"$[*].anomaly_type\", \"$[*].severity\", \"$[*].description\", \"$[*].is_confirmed\", \"$[*].confirmed_by\"]"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9093",
    "kafka_topic" = "iot-anomalies",
    "property.kafka_default_offsets" = "OFFSET_END"
);

-- commands to check routine load status
SHOW ROUTINE LOAD FOR iot_anomalies_load

STOP ROUTINE LOAD FOR iot_anomalies_load

RESUME ROUTINE LOAD FOR iot_anomalies_load




-- For a real-time line chart showing anomaly spikes
SELECT 
    DATE_TRUNC('minute', timestamp) AS minute_bucket,
    COUNT(*) AS anomaly_count,
    COUNT_IF(severity = 'high') AS critical_count
FROM 
    anomalies_streaming
WHERE 
    timestamp >= NOW() - INTERVAL '1 HOUR'
GROUP BY 
    1
ORDER BY 
    1 DESC
LIMIT 20;

-- For a priority-sorted bar chart
SELECT 
    device_id,
    anomaly_type,
    severity,
    is_confirmed
FROM 
    anomalies_streaming
WHERE 
    is_confirmed = false
    AND timestamp >= NOW() - INTERVAL '4 HOURS'
ORDER BY 
    CASE severity
        WHEN 'high' THEN 0
        WHEN 'medium' THEN 1
        ELSE 2
    END,
    timestamp DESC;


-- For a correlation matrix
SELECT 
    anomaly_type,
    severity,
    COUNT(*) AS count,
    AVG(TIMESTAMPDIFF('second', timestamp, NOW())) AS avg_age_seconds
FROM 
    anomalies_streaming
WHERE 
    timestamp >= NOW() - INTERVAL '1 DAY'
GROUP BY 
    1, 2;