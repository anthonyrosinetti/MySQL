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
    `DELETE FROM ${self()} WHERE DATE(date_report) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE();`
    )}
}

WITH all_ad_account_levels AS (
    SELECT
        DATE(date_report) AS date_report,
        customer_id,
        campaign_id,
        CAST(NULL AS STRING) AS ad_group_id,
        CAST(NULL AS STRING) AS ad_id,
        CAST(NULL AS STRING) AS keyword_id,
        device AS device_type,
        ad_network_type,
        impressions,
        clicks,
        cost_micros AS spend_micros
    FROM
        ${ref("stg_google_ads_campaign_basic_stats")}
    WHERE
        TRUE
        -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
        ${when(incremental(),
        `AND DATE(date_report) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE()`
        )}        

    UNION ALL

    SELECT
        DATE(date_report) AS date_report,
        customer_id,
        campaign_id,
        CASE WHEN deduplication_multiplicator = 1 THEN ad_group_id END AS ad_group_id,
        CAST(NULL AS STRING) AS ad_id,
        CAST(NULL AS STRING) AS keyword_id,
        device AS device_type,
        ad_network_type,
        deduplication_multiplicator * impressions AS impressions,
        deduplication_multiplicator * clicks AS clicks,
        deduplication_multiplicator * cost_micros AS spend_micros
    FROM
        ${ref("stg_google_ads_ad_group_basic_stats")}
    -- Ad group performance data are also counted at the campaign level, even though the precision is more accurate at the campaign level
    -- adding the ad group level without changing total at the campaign level
    -- using CROSS JOIN to duplicate the data, once with a +1 multiplicator, once with a -1 multiplicator, so the total is 0 (ie. not impacting the overall total)
    -- when multiplicator is +1, adding the ad group granularity dimension
    CROSS JOIN
        (SELECT * FROM UNNEST ([-1, 1]) AS deduplication_multiplicator)
    WHERE
        TRUE
        -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
        ${when(incremental(),
        `AND DATE(date_report) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE()`
        )}           

    UNION ALL

    SELECT
        DATE(date_report) AS date_report,
        customer_id,
        campaign_id,
        ad_group_id,
        CASE WHEN deduplication_multiplicator = 1 THEN ad_id END AS ad_id,
        CAST(NULL AS STRING) AS keyword_id,
        device AS device_type,
        ad_network_type,
        deduplication_multiplicator * impressions AS impressions,
        deduplication_multiplicator * clicks AS clicks,
        deduplication_multiplicator * cost_micros AS spend_micros
    FROM
        ${ref("stg_google_ads_ad_basic_stats")}
    -- Ad performance data are also counted at the ad group level, even though the precision is more accurate at the ad group level
    -- adding the ad level without changing total at the ad group level
    -- using CROSS JOIN to duplicate the data, once with a +1 multiplicator, once with a -1 multiplicator, so the total is 0 (ie. not impacting the overall total)
    -- when multiplicator is +1, adding the ad granularity dimension
    CROSS JOIN
        (SELECT * FROM UNNEST ([-1, 1]) AS deduplication_multiplicator)
    WHERE
        TRUE
        -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
        ${when(incremental(),
        `AND DATE(date_report) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE()`
        )}           

    UNION ALL

    SELECT
        DATE(date_report) AS date_report,
        customer_id,
        campaign_id,
        ad_group_id,
        CAST(NULL AS STRING) AS ad_id,
        CASE WHEN deduplication_multiplicator = 1 THEN keyword_id END AS keyword_id,
        device AS device_type,
        ad_network_type,
        deduplication_multiplicator * impressions AS impressions,
        deduplication_multiplicator * clicks AS clicks,
        deduplication_multiplicator * cost_micros AS spend_micros
    FROM
        ${ref("stg_google_ads_keyword_basic_stats")}
    -- Keyword performance data are also counted at the ad group level, even though the precision is more accurate at the ad group level
    -- adding the keyword level without changing total at the ad group level
    -- using CROSS JOIN to duplicate the data, once with a +1 multiplicator, once with a -1 multiplicator, so the total is 0 (ie. not impacting the overall total)
    -- when multiplicator is +1, adding the keyword granularity dimension
    CROSS JOIN
        (SELECT * FROM UNNEST ([-1, 1]) AS deduplication_multiplicator)
    WHERE
        TRUE
        -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
        ${when(incremental(),
        `AND DATE(date_report) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE()`
        )}           
)

SELECT
    date_report,
    customer_id,
    campaign_id,
    ad_group_id,
    ad_id,
    keyword_id,
    device_type,
    ad_network_type,
    NULLIF(SUM(impressions), 0) AS impressions,
    NULLIF(SUM(clicks), 0) AS clicks,
    -- Switching to micro-currency to standard currency
    NULLIF(ROUND(SUM(spend_micros) / 1000000, 2), 0) AS spend
FROM
    all_ad_account_levels
GROUP BY
    date_report,
    customer_id,
    campaign_id,
    ad_group_id,
    ad_id,
    keyword_id,
    device_type,
    ad_network_type
HAVING
    impressions IS NOT NULL
    OR clicks IS NOT NULL
    OR spend IS NOT NULL
