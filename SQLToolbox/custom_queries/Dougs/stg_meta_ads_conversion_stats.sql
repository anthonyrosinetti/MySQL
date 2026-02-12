{{ config(
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
        "field": "date_report",
        "data_type": "date",
        "granularity": "day"
    }
) }}

SELECT
    date_start AS date_report,
    account_id,
    campaign_id,
    SUM(CASE WHEN NULLIF(LAX_STRING(JSON_QUERY(actions, "$.action_type")), "") = 'YYYY' THEN CAST(JSON_VALUE(actions, "$.value") AS INT64) ELSE 0 END) AS actions,
    CAST(NULL AS INT64) AS conversions
FROM
    {{ source('bronze_airbyte_facebook_ads', 'ads_insights') }}
{% if is_incremental() %}
WHERE
    -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
    date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE()
{% endif %}   
GROUP BY
    date_report,
    account_id,
    campaign_id       

UNION ALL

SELECT
    date_start AS date_report,    
    account_id,
    campaign_id,   
    CAST(NULL AS INT64) AS actions,
    SUM(CASE WHEN NULLIF(LAX_STRING(JSON_QUERY(conversions, "$.action_type")), "") = 'YYYY' THEN CAST(JSON_VALUE(conversions, "$.value") AS INT64) ELSE 0 END) AS conversions
FROM
    {{ source('bronze_airbyte_facebook_ads', 'ads_insights') }}
{% if is_incremental() %}
WHERE
    -- Due to important data volume in some Google Ads table, using incremental refresh strategy ; data is considered as stable after around 30 days
    date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 35 DAY) AND CURRENT_DATE()
{% endif %}     
GROUP BY
    date_report,
    account_id,
    campaign_id  
