{{ config(
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

WITH layer_1 AS (
    SELECT
        TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
        user_pseudo_id,
        CONCAT("U", user_pseudo_id, "-S", (SELECT value.int_value FROM UNNEST(event_params) WHERE key = "ga_session_id")) AS session_id,
        event_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "source") AS event_param_source,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "medium") AS event_param_medium,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "campaign") AS event_param_campaign,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "content") AS event_param_content,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "term") AS event_param_term,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "gclid") AS event_param_gclid,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "fbclid") AS event_param_fbclid,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "utm_source") AS event_param_utm_source,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "utm_medium") AS event_param_utm_medium,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "utm_campaign") AS event_param_utm_campaign,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "utm_id") AS event_param_utm_id,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "utm_content") AS event_param_utm_content,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "utm_term") AS event_param_utm_term,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_location") AS page_location,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = "page_referrer") AS page_referrer,
        collected_traffic_source,
        session_traffic_source_last_click
    FROM
        {{ ref('stg_google_analytics_4_events') }}
    WHERE
        user_pseudo_id IS NOT NULL
        {% if is_incremental() %} -- Adding an extra day to the date range to consider cross-day session here (but will be removed at the end)
        AND table_suffix_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 + 1 DAY) AND CURRENT_DATE()
        {% endif %}
),

session_attributes_l1 AS (
    SELECT
        MIN(event_timestamp) OVER (PARTITION BY user_pseudo_id, session_id) AS session_start_timestamp,
        user_pseudo_id,
        session_id,
        event_timestamp,
        COALESCE(event_param_utm_source, event_param_source) AS event_param_utm_source,
        COALESCE(event_param_utm_medium, event_param_medium) AS event_param_utm_medium,
        COALESCE(event_param_utm_campaign, event_param_campaign) AS event_param_utm_campaign,
        event_param_utm_id,
        COALESCE(event_param_utm_content, event_param_content) AS event_param_utm_content,
        COALESCE(event_param_utm_term, event_param_term) AS event_param_term,
        COALESCE(event_param_gclid, REGEXP_EXTRACT(page_location, r"[\?&]gclid=([^&]*)")) AS event_param_gclid,
        COALESCE(event_param_fbclid, REGEXP_EXTRACT(page_location, r"[\?&]fbclid=([^&]*)")) AS event_param_fbclid,
        page_location,
        page_referrer,
        collected_traffic_source,
        session_traffic_source_last_click
    FROM
        layer_1
),

session_attributes_l2 AS (
    SELECT
        session_start_timestamp,
        user_pseudo_id,
        session_id,
        event_timestamp,
        CASE
            WHEN event_param_gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.manual_source IS NOT NULL THEN collected_traffic_source.manual_source
            WHEN event_param_utm_source IS NOT NULL THEN event_param_utm_source
            WHEN session_traffic_source_last_click.manual_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.manual_campaign.`source`
            WHEN session_traffic_source_last_click.google_ads_campaign.campaign_id IS NOT NULL THEN "google"
            WHEN session_traffic_source_last_click.cross_channel_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.cross_channel_campaign.`source`
        END AS utm_source,
        CASE
            WHEN event_param_gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.manual_source IS NOT NULL THEN collected_traffic_source.manual_medium
            WHEN event_param_utm_source IS NOT NULL THEN event_param_utm_medium
            WHEN session_traffic_source_last_click.manual_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.manual_campaign.medium
            WHEN session_traffic_source_last_click.google_ads_campaign.campaign_id IS NOT NULL THEN "cpc"
            WHEN session_traffic_source_last_click.cross_channel_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.cross_channel_campaign.medium
        END AS utm_medium,
        CASE
            WHEN event_param_gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.manual_source IS NOT NULL THEN collected_traffic_source.manual_campaign_name
            WHEN event_param_utm_source IS NOT NULL THEN event_param_utm_campaign
            WHEN session_traffic_source_last_click.manual_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.manual_campaign.campaign_name
            WHEN session_traffic_source_last_click.google_ads_campaign.campaign_id IS NOT NULL THEN session_traffic_source_last_click.google_ads_campaign.campaign_name
            WHEN session_traffic_source_last_click.cross_channel_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.cross_channel_campaign.campaign_name
        END AS utm_campaign,
        CASE
            WHEN event_param_gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.manual_source IS NOT NULL THEN collected_traffic_source.manual_campaign_id
            WHEN event_param_utm_source IS NOT NULL THEN event_param_utm_id
            WHEN session_traffic_source_last_click.manual_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.manual_campaign.campaign_id
            WHEN session_traffic_source_last_click.google_ads_campaign.campaign_id IS NOT NULL THEN session_traffic_source_last_click.google_ads_campaign.campaign_id
            WHEN session_traffic_source_last_click.cross_channel_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.cross_channel_campaign.campaign_id
        END AS utm_id,
        CASE
            WHEN event_param_gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.manual_source IS NOT NULL THEN collected_traffic_source.manual_content
            WHEN event_param_utm_source IS NOT NULL THEN event_param_utm_content
            WHEN session_traffic_source_last_click.manual_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.manual_campaign.content
            WHEN session_traffic_source_last_click.google_ads_campaign.campaign_id IS NOT NULL THEN session_traffic_source_last_click.google_ads_campaign.ad_group_name
            WHEN session_traffic_source_last_click.cross_channel_campaign.`source` IS NOT NULL THEN CAST(NULL AS STRING)
        END AS utm_content,
        CASE
            WHEN event_param_gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.manual_source IS NOT NULL THEN collected_traffic_source.manual_term
            WHEN event_param_utm_source IS NOT NULL THEN event_param_utm_term
            WHEN session_traffic_source_last_click.manual_campaign.`source` IS NOT NULL THEN session_traffic_source_last_click.manual_campaign.term
            WHEN session_traffic_source_last_click.google_ads_campaign.campaign_id IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN session_traffic_source_last_click.cross_channel_campaign.`source` IS NOT NULL THEN CAST(NULL AS STRING)
        END AS utm_term,
        CASE
            WHEN event_param_gclid IS NOT NULL THEN event_param_gclid
            WHEN collected_traffic_source.gclid IS NOT NULL THEN collected_traffic_source.gclid
            WHEN collected_traffic_source.manual_source IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN event_param_utm_source IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN session_traffic_source_last_click.manual_campaign.`source` IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN session_traffic_source_last_click.google_ads_campaign.campaign_id IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN session_traffic_source_last_click.cross_channel_campaign.`source` IS NOT NULL THEN CAST(NULL AS STRING)
        END AS gclid,
        CASE
            WHEN event_param_gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.gclid IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN collected_traffic_source.manual_source IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN event_param_utm_source IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN session_traffic_source_last_click.manual_campaign.`source` IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN session_traffic_source_last_click.google_ads_campaign.campaign_id IS NOT NULL THEN CAST(NULL AS STRING)
            WHEN session_traffic_source_last_click.cross_channel_campaign.`source` IS NOT NULL THEN CAST(NULL AS STRING)
            ELSE fbclid
        END AS fbclid,
        page_location,
        page_referrer,
    FROM
        session_attributes_l1
),

session_attributes_l3 AS (
    SELECT
        session_start_timestamp,
        user_pseudo_id,
        session_id,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_id,
        utm_content,
        utm_term,
        gclid,
        fbclid,
        page_location,
        page_referrer,
    FROM
        session_attributes_l2
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY user_pseudo_id, session_id
            ORDER BY event_timestamp ASC
        ) = 1
),

session_attributes_l4 AS (
    SELECT
        ses.session_start_timestamp,
        ses.user_pseudo_id,
        ses.session_id,
        CASE
            WHEN gclid IS NOT NULL THEN "google"
            ELSE ses.utm_source
        END AS utm_source,
        CASE
            WHEN gclid IS NOT NULL THEN "cpc"
            ELSE ses.utm_medium
        END AS utm_medium,
        CASE
            WHEN gclid IS NOT NULL THEN gcl.campaign_name
            ELSE ses.utm_campaign
        END AS utm_campaign,
        CASE
            WHEN gclid IS NOT NULL THEN gcl.campaign_id
            ELSE ses.utm_id
        END AS utm_id,
        CASE
            WHEN gclid IS NOT NULL THEN gcl.ad_group_name
            ELSE ses.utm_content
        END AS utm_content,
        CASE
            WHEN gclid IS NOT NULL THEN gcl.keyword_name
            ELSE ses.utm_term
        END AS utm_term,
        ses.gclid,
        ses.fbclid,
        ses.page_location AS landing_page,
        ses.page_referrer AS referrer,
    FROM
        session_attributes_l3 AS ses
    LEFT JOIN
        {{ ref('fct_google_ads_clicks') }} AS gcl
    ON
        ses.gclid = gcl.gclid
)

SELECT
    DATE(session_start_timestamp) AS date,
    session_start_timestamp,
    user_pseudo_id,
    session_id,
    REGEXP_EXTRACT(landing_page, r"^(?:https?:\/\/)?(\/[^?#]*)(?:[?#].*)?$") AS landing_page,
    REGEXP_EXTRACT(referrer, r"^(?:https?:\/\/)?([^\/]+)(?:\/.*)?$") AS referrer,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_id,
    utm_content,
    utm_term,
    gclid,
    fbclid,
    REGEXP_EXTRACT(landing_page, r"[\?&]r=([^&]*)") AS partner_referral_id,
FROM
    session_attributes_l4
WHERE
    TRUE
    {% if is_incremental() %} -- Removing cross day session from day before date range
    AND DATE(session_start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()
    {% endif %}
