{{ config(
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
        "field": "date_report",
        "data_type": "date",
        "granularity": "day"
    }
) }}

WITH ga4_events AS (
    -- Extracting data from the nested event params
    SELECT
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        user_pseudo_id,
        CONCAT("U", user_pseudo_id, "-S", (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id")) AS session_id,
        event_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location") AS page_location,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_referrer") AS page_referrer
    FROM
        {{ ref('stg_google_analytics_4_events') }}
    WHERE
        user_pseudo_id IS NOT NULL
        {% if is_incremental() %}
        -- Due to important data volume, using incremental refresh strategy ; data is considered as stable after around 3 days
        -- Adding an extra day to the date range to consider cross-day session here (but will be removed at the end)
        AND table_suffix_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 + 1 DAY) AND CURRENT_DATE()
        {% endif %}
),

first_pageviews_calculation AS (
    SELECT DISTINCT
        event_timestamp AS first_page_timestamp,
        user_pseudo_id,
        page_location AS first_page_location,
        page_referrer AS first_page_referrer
    FROM
        ga4_events
    WHERE
        event_name LIKE "%page%view%"
        AND
        REGEXP_EXTRACT(page_location, r"^(?:https?://)?([^/?#]+)") = 'www.dougs.fr'        
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY user_pseudo_id
            ORDER BY event_timestamp ASC
        ) = 1
),

first_pageviews_decoding AS (
    SELECT
        DATE(first_page_timestamp) AS date_report,
        first_page_timestamp,
        user_pseudo_id,
        -- Removing protocol as well as URL parameters / anchors
        REGEXP_EXTRACT(first_page_location, r"^(?:https?://)?[^/]+(/[^?#]*)") AS first_page,
        REGEXP_EXTRACT(first_page_referrer, r"^(?:https?://)?[^/]+(/[^?#]*)") AS first_referrer
    FROM
        first_pageviews_calculation
)

SELECT
    *,
    CASE
        WHEN STARTS_WITH(first_page,'/blog/') THEN 'Blog'
        WHEN STARTS_WITH(first_page,'/webinar') THEN 'Webinar'
        WHEN STARTS_WITH(first_page,'/videos') THEN 'Video'
        WHEN STARTS_WITH(first_page,'/guide') THEN 'Guide'
        WHEN STARTS_WITH(first_page,'/ressources/') THEN 'Ressource'
        WHEN STARTS_WITH(first_page,'/temoignage') THEN 'Témoignage'
        WHEN STARTS_WITH(first_page,'/outils-simulation/') THEN 'Outil de simulation'
        WHEN STARTS_WITH(first_page,'/expert-comptable/') THEN 'Expert-comptable'
        WHEN STARTS_WITH(first_page,'/parten') THEN 'Partenariat'
        WHEN STARTS_WITH(first_page,'/cre') THEN 'Création'
        WHEN STARTS_WITH(first_page,'/facturation-electronique/') THEN 'Facturation électronique'
        ELSE '-'
    END AS first_page_category
FROM
    first_pageviews_decoding
WHERE
    TRUE
    {% if is_incremental() %}
    -- Removing cross day session from day before date range
    AND date_report BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()
    {% endif %}    
