SELECT
   utm_campaign,
   utm_source,
   utm_medium,
   campaign_group,
   CASE
        WHEN utm_campaign IS NOT NULL AND utm_source IS NOT NULL AND utm_medium IS NOT NULL THEN 1
        WHEN utm_campaign IS NOT NULL AND utm_source IS NOT NULL AND utm_medium IS NULL THEN 2
        WHEN utm_campaign IS NOT NULL AND utm_source IS NULL AND utm_medium IS NOT NULL THEN 3
        WHEN utm_campaign IS NOT NULL AND utm_source IS NULL AND utm_medium IS NULL THEN 4
        WHEN utm_campaign IS NULL AND utm_source IS NOT NULL AND utm_medium IS NOT NULL THEN 5
        WHEN utm_campaign IS NULL AND utm_source IS NOT NULL AND utm_medium IS NULL THEN 6
        WHEN utm_campaign IS NULL AND utm_source IS NULL AND utm_medium IS NOT NULL THEN 7
        WHEN utm_campaign IS NULL AND utm_source IS NULL AND utm_medium IS NULL THEN 8
        ELSE 9
    END AS sort
FROM {{ ref('mapping_campaign') }}
