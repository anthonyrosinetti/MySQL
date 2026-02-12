-- Keeping only the latest data per campaign to fit with what's available in the platform UI
SELECT
    date_last_refresh,
    customer_id,
    campaign_id,
    campaign_name,
    advertising_channel_type,
    advertising_channel_sub_type,
    CASE
        WHEN advertising_channel_type = "DEMAND_GEN" THEN "Demand Gen"
        WHEN advertising_channel_type = "DISPLAY" THEN "Display"
        WHEN advertising_channel_type = "HOTEL" THEN "Hotel"
        WHEN advertising_channel_type = "LOCAL" THEN "Local"
        WHEN advertising_channel_type = "LOCAL_SERVICES" THEN "Local services"
        WHEN advertising_channel_type = "MULTI_CHANNEL" THEN "App"
        WHEN advertising_channel_type = "PERFORMANCE_MAX" THEN "Performance Max"
        WHEN advertising_channel_type = "SEARCH" THEN "Search"
        WHEN advertising_channel_type = "SHOPPING" THEN "Shopping"
        WHEN advertising_channel_type = "SMART" THEN "Smart"
        WHEN advertising_channel_type = "TRAVEL" THEN "Travel"
        WHEN advertising_channel_type = "VIDEO" THEN "Video"
        ELSE INITCAP(advertising_channel_type)
    END AS campaign_type
FROM
    ${ref("stg_google_ads_campaign")}
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY customer_id, campaign_id
        ORDER BY date_last_refresh DESC
    ) = 1
