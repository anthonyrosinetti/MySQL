SELECT
   utm_source,
   utm_medium,
   source_name,
   channel,
   CASE
        WHEN utm_source IS NOT NULL AND utm_medium IS NOT NULL THEN 1
        WHEN utm_source IS NOT NULL AND utm_medium IS NULL THEN 2
        WHEN utm_source IS NULL AND utm_medium IS NOT NULL THEN 3
        ELSE 4
    END AS sort
FROM {{ ref('mapping_source_medium') }}
