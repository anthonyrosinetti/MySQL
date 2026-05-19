-- Config block
config {
    type: "incremental",
    protected: true,
    partitionBy: "date",
    schema: "tracking_monitoring" 
}

-- Pre-operations
pre_operations {
    ${when(incremental(),
    `DELETE FROM ${self()} WHERE DATE(date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND CURRENT_DATE();`
    )}
}

-- SQL
SELECT DISTINCT
    DATE(date) AS date,
    site_id,
    event_name,
FROM
    ${ref("piano_analytics_raw_events")}
WHERE
    TRUE
    ${when(incremental(),
    `AND DATE(date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND CURRENT_DATE()`
    )}
