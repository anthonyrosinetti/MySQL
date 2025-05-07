WITH raw_data AS (
    SELECT DISTINCT
        external_id AS user_id,
        LAST_VALUE(id) OVER w AS intercom_id,
        LAST_VALUE(location) OVER w AS location,
    FROM
        `{destination_project_id}.{destination_dataset_id}.intercom_user_location_history`
    WINDOW
        w AS (
            PARTITION BY external_id
            ORDER BY _ingestion_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),

synchro AS (
    SELECT
        id AS intercom_id,
        external_id AS user_id,
        STRUCT(
            location.type,
            location.country,
            location.region,
            location.city
        ) AS location, 
    FROM
        `{source_project_id}.{source_dataset_id}.intercom_raw_contacts`
)

SELECT
    synchro.*,
    CURRENT_TIMESTAMP() AS _ingestion_timestamp
FROM
    synchro
LEFT JOIN
    raw_data
USING
    (user_id)
WHERE
    IFNULL(synchro.intercom_id, "") != IFNULL(raw_data.intercom_id, "")
    OR IFNULL(synchro.location.type, "") != IFNULL(raw_data.location.type, "")
    OR IFNULL(synchro.location.country, "") != IFNULL(raw_data.location.country, "")
    OR IFNULL(synchro.location.region, "") != IFNULL(raw_data.location.region, "")
    OR IFNULL(synchro.location.city, "") != IFNULL(raw_data.location.city, "")
