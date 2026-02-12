config {
    type: "incremental",
    schema: "preanalysis",
    partitionBy: "date_last_refresh",
    requirePartitionFilter: false,
    protected: true
}

-- Pre-operations
pre_operations {
    ${when(incremental(),
    `DELETE FROM ${self()} WHERE DATE(_DATA_DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE();`
    )}
}

SELECT
    _DATA_DATE AS date_last_refresh,
    CAST(customer_id AS STRING) AS customer_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(ad_group_id AS STRING) AS ad_group_id,
    ad_group_name
FROM
    ${ref("ads_AdGroup_9369677276")}
WHERE
    TRUE
    -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
    ${when(incremental(),
    `AND DATE(_DATA_DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE()`
    )}
