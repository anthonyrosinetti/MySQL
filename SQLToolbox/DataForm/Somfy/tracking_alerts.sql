-- Config block
config {
  type: "incremental",
  protected: true,
  partitionBy: "TIMESTAMP_TRUNC(_ingestion_timestamp, DAY)",
  schema: "tracking_monitoring" 
}

-- SQL
WITH events_scope AS (
    SELECT
        *
    FROM
        ${ref("piano_analytics_raw_events")}
    WHERE
        date BETWEEN DATE_SUB(CURRENT_DATE(),INTERVAL 8 DAY) AND CURRENT_DATE()
),

last_alert AS (
    SELECT DISTINCT
        event_name,
        website,
        LAST_VALUE(_ingestion_timestamp) OVER w AS last_alert_timestamp,
        -- LAST_VALUE(previous_alert_timestamp) OVER w AS previous_alert_timestamp,
        LAST_VALUE(first_alert_timestamp) OVER w AS first_alert_timestamp,
        -- LAST_VALUE(is_new_alert) OVER w AS is_new_alert
    FROM
        ${self()}
    WINDOW
        w AS (
            PARTITION BY event_name, website
            ORDER BY _ingestion_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),

all_events AS (
    SELECT DISTINCT
        event_name,
        site_id,
        website
    FROM
        ${ref("piano_analytics_raw_events")}
    LEFT JOIN
        ${ref("site_id_websites")}
    USING
        (site_id)     
    WHERE
        event_name NOT LIKE '%exclusion%'
        AND
        date BETWEEN DATE_SUB(CURRENT_DATE(),INTERVAL 6 MONTH) AND CURRENT_DATE()
),

synchro AS (
    SELECT
        a.event_name,
        a.website,
        l.last_alert_timestamp,
        DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) AS monitored_date,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) THEN e.m_events ELSE 0 END) AS minus_1_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY) THEN e.m_events ELSE 0 END) AS minus_2_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 3 DAY) THEN e.m_events ELSE 0 END) AS minus_3_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 4 DAY) THEN e.m_events ELSE 0 END) AS minus_4_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 5 DAY) THEN e.m_events ELSE 0 END) AS minus_5_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 6 DAY) THEN e.m_events ELSE 0 END) AS minus_6_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 7 DAY) THEN e.m_events ELSE 0 END) AS minus_7_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 8 DAY) THEN e.m_events ELSE 0 END) AS minus_8_event_count
    FROM
        all_events a
    LEFT JOIN
        events_scope e
    ON
        e.event_name = a.event_name
        AND
        e.site_id = a.site_id
    LEFT JOIN
        last_alert l
    ON
        l.event_name = a.event_name
        AND
        l.website = a.website     
    GROUP BY
        event_name,
        website,
        last_alert_timestamp
    HAVING
        minus_1_event_count = 0
)

SELECT
    'zero_event' AS alert_type,
    synchro.event_name,
    CAST(NULL AS STRING) AS metric_name,
    CAST(NULL AS ARRAY<STRING>) AS event_parameters,
    synchro.website,
    DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) AS monitored_date,
    CAST(NULL AS STRING) AS comparison_date_range,
    CURRENT_TIMESTAMP() AS _ingestion_timestamp,
    True AS is_new_alert,
    True AS forced_notification,
    CAST(NULL AS TIMESTAMP) AS previous_alert_timestamp,
    CURRENT_TIMESTAMP() AS first_alert_timestamp,
    CONCAT("L\'événement ",event_name," a été collecté ",minus_1_event_count," fois sur la journée du ",monitored_date) AS rationale
FROM
    synchro
WHERE
    synchro.minus_1_event_count = 0
    AND
    synchro.minus_2_event_count > 0
    AND
    DATE_DIFF(DATE(CURRENT_TIMESTAMP()),DATE(last_alert_timestamp), DAY) >= 1    

UNION ALL

SELECT
    'zero_event' AS alert_type,
    synchro.event_name,
    CAST(NULL AS STRING) AS metric_name,
    CAST(NULL AS ARRAY<STRING>) AS event_parameters,
    synchro.website,
    DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) AS monitored_date,
    CAST(NULL AS STRING) AS comparison_date_range,
    CURRENT_TIMESTAMP() AS _ingestion_timestamp,
    False AS is_new_alert,
    True AS forced_notification,
    last_alert.last_alert_timestamp AS previous_alert_timestamp,
    first_alert_timestamp,
    CONCAT("L\'événement ",event_name," a été collecté ",minus_1_event_count," fois sur la journée du ",monitored_date) AS rationale
FROM
    synchro
LEFT JOIN
    last_alert
USING
    (event_name,website)
WHERE
    synchro.minus_1_event_count = 0
    AND
    synchro.minus_2_event_count = 0
    AND
    synchro.minus_3_event_count = 0
    AND
    synchro.minus_4_event_count = 0
    AND
    synchro.minus_5_event_count = 0
    AND
    synchro.minus_6_event_count = 0
    AND
    synchro.minus_7_event_count = 0
    AND
    synchro.minus_8_event_count = 0
    AND
    DATE_DIFF(DATE(CURRENT_TIMESTAMP()),DATE(last_alert.last_alert_timestamp), DAY) >= 7
