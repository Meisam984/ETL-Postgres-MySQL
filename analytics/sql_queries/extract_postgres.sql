WITH
    ext_dev AS (
        SELECT
            id, device_id,
            time AS epoch,
            TO_CHAR(TO_TIMESTAMP(time::NUMERIC), 'YY-MM-DD HH24:') AS timestamp,
            temperature,
            CAST(location as JSON) AS location
        FROM devices
        WHERE id > {}
    ),
    formatted_ext_dev AS (
        SELECT
            *,
            LEAD(location) OVER(
                PARTITION BY device_id, timestamp
            ) AS next_loc,
            MAX(temperature) OVER(
                PARTITION BY device_id, timestamp
            ) AS max_dev_temp,
            COUNT(1) OVER (
                PARTITION BY device_id, timestamp
            ) AS dev_data_count
        FROM ext_dev
    )
SELECT
    id, device_id, epoch, timestamp, temperature, location,
    CAST(next_loc AS JSON) AS next_loc,
    max_dev_temp, dev_data_count
FROM formatted_ext_dev;