config {
    type: "incremental",
    schema: "preanalysis",
    partitionBy: "date_report",
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
    _DATA_DATE AS date_report,
    CAST(customer_id AS STRING) AS customer_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    CAST(ad_group_id AS STRING) AS ad_group_id,
    click_view_ad_group_ad AS ad_group_id_ad_id,
    click_view_keyword AS ad_group_id_keyword_id,
    click_view_gclid AS gclid
FROM
    ${ref("ads_ClickStats_9369677276")}
WHERE
    TRUE
    -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
    ${when(incremental(),
    `AND DATE(_DATA_DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE()`
    )}
