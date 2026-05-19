-- Config block
config {
    type: "incremental",
    schema: "tracking_monitoring",
    protected: true,
    partitionBy: "date",
    tags: ["daily_run"]
}

-- Pre-operations
pre_operations {
    ${when(incremental(),
    `DELETE FROM ${self()} WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE();`
    )}
}

-- SQL
SELECT
    e.date,
    e.event_name,
    e.click,
    cf.website,
    SUM(e.m_events) AS event_count
FROM
    ${ref("piano_analytics_raw_events")} AS e
LEFT JOIN
    ${ref("piano_websites")} AS cf
USING
    (site_id)
WHERE
    IFNULL(cf.tracking_alert_status, FALSE) = TRUE
    ${when(incremental(),
    `AND e.date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()`
    )}
GROUP BY
    date,
    event_name,
    click,
    website
