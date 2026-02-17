-- Keeping only the latest data per campaign to fit with what's available in the platform UI
SELECT
    timestamp_last_refresh,
    REGEXP_EXTRACT(account_urn, r"^urn:li:sponsoredAccount:([0-9]+)$") AS account_id,
    REGEXP_EXTRACT(campaign_group_urn, r"^urn:li:sponsoredCampaignGroup:([0-9]+)$") campaign_group_id,
    campaign_id,
    campaign_name
FROM
    {{ ref('stg_linkedin_ads_campaigns') }}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY campaign_id
        ORDER BY timestamp_last_refresh DESC
    ) = 1
