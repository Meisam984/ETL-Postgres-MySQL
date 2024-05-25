SELECT
    device_id, timestamp,
    MAX(max_dev_temp) AS max_dev_temp,
    SUM(dev_data_count) AS dev_data_count,
    SUM(total_movement) AS total_movement
FROM raw_analytics
WHERE id > {}
GROUP BY device_id, timestamp;