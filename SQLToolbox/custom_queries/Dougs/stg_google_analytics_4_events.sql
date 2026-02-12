config {
    type: "incremental",
    schema: "preanalysis",
    partitionBy: "event_date",
    requirePartitionFilter: false,
    protected: true
}

-- Pre-operations
pre_operations {
    ${when(incremental(),
    `DELETE FROM ${self()} WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 + 1 DAY) AND CURRENT_DATE();`
    )}
}

SELECT
    PARSE_DATE("%Y%m%d", event_date) AS event_date,
    event_timestamp,
    user_pseudo_id,
    event_name,
    event_params,
    collected_traffic_source,
    session_traffic_source_last_click
FROM
    ${ref("events_*")}
WHERE
    TRUE
    -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
    ${when(incremental(),
    `AND event_date BETWEEN FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 3 + 1 DAY)) AND FORMAT_DATE("%Y%m%d", CURRENT_DATE())`
    )}
