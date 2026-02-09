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
    SELECT
        event_timestamp AS first_page_timestamp,
        user_pseudo_id,
        page_location AS first_page_location,
        page_referrer AS first_page_referrer
    FROM
        ga4_events
    WHERE
        event_name LIKE "%page%view%"
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY user_pseudo_id
            ORDER BY event_timestamp ASC
        ) = 1
)

SELECT
    DATE(first_page_timestamp) AS date_report,
    first_page_timestamp,
    user_pseudo_id,
    -- Removing protocol as well as URL parameters / anchors
    REGEXP_EXTRACT(first_page_location, r"^(?:https?://)?[^/]+(/[^?#]*)") AS first_page,
    REGEXP_EXTRACT(first_page_referrer, r"^(?:https?://)?[^/]+(/[^?#]*)") AS first_referrer
FROM
    first_pageviews_calculation
WHERE
    TRUE
    {% if is_incremental() %}
    -- Removing cross day session from day before date range
    AND DATE(session_start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()
    {% endif %}
