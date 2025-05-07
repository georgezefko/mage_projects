--Maintenance Priority Pie Chart

sql
SELECT 
    priority,
    COUNT(*) AS device_count,
    SUM(estimated_cost) AS estimated_cost
FROM 
    gold_maintenance_schedule
WHERE 
    date = CURRENT_DATE()
GROUP BY 
    1
ORDER BY 
    CASE priority
        WHEN 'P0' THEN 0
        WHEN 'P1' THEN 1
        WHEN 'P2' THEN 2
        ELSE 3
    END;


--Risk Trend Line Chart

sql
SELECT 
    date,
    AVG(failure_risk_score) AS avg_risk,
    PERCENTILE(failure_risk_score, 0.9) AS p90_risk
FROM 
    gold_device_health
WHERE 
    device_id = 'DEVICE_X' 
    AND date >= CURRENT_DATE() - INTERVAL '7 DAYS'
GROUP BY 
    1
ORDER BY 
    date;

--Anomaly Heatmap

sql
SELECT 
    FLOOR(max_temp/5)*5 AS temp_bucket,
    FLOOR(vibration_anomalies/2)*2 AS vibration_bucket,
    COUNT(*) AS device_count,
    AVG(failure_risk_score) AS avg_risk
FROM 
    gold_device_health
WHERE 
    date = CURRENT_DATE() - INTERVAL '1 DAY'
GROUP BY 
    1, 2
ORDER BY 
    1, 2;


--Cost Waterfall

sql
WITH cost_summary AS (
    SELECT 
        priority,
        SUM(estimated_cost) AS cost
    FROM 
        gold_maintenance_schedule
    WHERE 
        date BETWEEN CURRENT_DATE() AND CURRENT_DATE() + INTERVAL '7 DAYS'
    GROUP BY 
        1
)
SELECT 
    'P0 Emergency' AS category,
    cost
FROM 
    cost_summary 
WHERE 
    priority = 'P0'
UNION ALL
SELECT 
    'P1 Urgent',
    cost
FROM 
    cost_summary 
WHERE 
    priority = 'P1'
UNION ALL
SELECT 
    'P2 Routine',
    cost
FROM 
    cost_summary 
WHERE 
    priority = 'P2'
UNION ALL
SELECT 
    'Total',
    SUM(cost)
FROM 
    cost_summary;


--Maintenance Gantt Data

sql
SELECT 
    device_id,
    priority,
    CURRENT_DATE() AS start_date,
    CASE 
        WHEN priority = 'P0' THEN CURRENT_DATE() + INTERVAL '1 DAY'
        WHEN priority = 'P1' THEN CURRENT_DATE() + INTERVAL '3 DAYS'
        ELSE CURRENT_DATE() + INTERVAL '7 DAYS'
    END AS target_date
FROM 
    gold_maintenance_schedule
WHERE 
    date = CURRENT_DATE()
ORDER BY 
    priority,
    days_since_maintenance DESC;