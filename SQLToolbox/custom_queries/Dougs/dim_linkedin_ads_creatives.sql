-- Keeping only the latest data per creative to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    REGEXP_EXTRACT(account_urn, r"^urn:li:sponsoredAccount:([0-9]+)$") AS account_id,
    REGEXP_EXTRACT(campaign_urn, r"^urn:li:sponsoredCampaign:([0-9]+)$") AS campaign_id,
    REGEXP_EXTRACT(creative_urn, r"^urn:li:sponsoredCreative:([0-9]+)$") AS creative_id,
    creative_name
FROM
    {{ ref('stg_linkedin_ads_creatives') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY creative_urn
        ORDER BY timestamp_last_refresh DESC
    ) = 1
