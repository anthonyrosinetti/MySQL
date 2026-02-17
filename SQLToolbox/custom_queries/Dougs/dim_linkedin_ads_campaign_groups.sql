-- Keeping only the latest data per campaign group to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    REGEXP_EXTRACT(account_urn, r"^urn:li:sponsoredAccount:([0-9]+)$") AS account_id,
    campaign_group_id,
    campaign_group_name
FROM
    {{ ref('stg_linkedin_ads_campaign_groups') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY campaign_group_id
        ORDER BY timestamp_last_refresh DESC
    ) = 1
