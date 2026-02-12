{{ config(
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

SELECT
    _DATA_DATE AS date,
    campaign_id,
    SUM(CASE WHEN segments_conversion_action = 'customers/8122074940/conversionActions/XXXXXXXXX' THEN metrics_all_conversions ELSE 0 END) AS conversions
FROM
     {{ source('bronze_google_ads', 'ads_CampaignCrossDeviceConversionStats_8122074940') }}
{% if is_incremental() %}
WHERE
    -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
    DATE(_DATA_DATE) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE()
{% endif %}
GROUP BY
    date,
    campaign_id
   
