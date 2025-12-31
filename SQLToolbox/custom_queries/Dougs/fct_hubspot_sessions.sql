{{ config(
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    }
) }}

WITH events AS (
    SELECT
        hs_event_id,
        hs_contact_id,
        event_timestamp,
        LAG(event_timestamp) OVER w AS previous_event_timestamp,
        LAG(hs_event_id) OVER w AS previous_hs_event_id,
        referrer,
        url,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term
    FROM
        {{ ref('stg_airbyte_hubspot_event_visited_page') }}
    WHERE
        hs_contact_id IS NOT NULL
    WINDOW
        w AS (
            PARTITION BY hs_contact_id
            ORDER BY event_timestamp ASC
        )
)

SELECT
    DATE(event_timestamp) AS date,
    hs_event_id AS session_id,
    hs_contact_id AS contact_id,
    event_timestamp AS session_start_timestamp,
    utm_source,
    utm_medium,
    utm_campaign,
    REGEXP_EXTRACT(url, r"[\?&]utm_id=([^&]*)") AS utm_id,
    utm_content,
    utm_term,
    referrer,
    REGEXP_EXTRACT(url, r"^(?:https?:\/\/)?(\/[^?#]*)(?:[?#].*)?$") AS landing_page,
    REGEXP_EXTRACT(url, r"[\?&]gclid=([^&]*)") AS gclid,
    REGEXP_EXTRACT(url, r"[\?&]fbclid=([^&]*)") AS fbclid,
    REGEXP_EXTRACT(url, r"[\?&]r=([^&]*)") AS partner_referral_id
FROM
    events
WHERE
    (-- Criterias defining a session start:
        previous_hs_event_id IS NULL -- First event of the contact on the date range
        OR TIMESTAMP_DIFF(event_timestamp, previous_event_timestamp, MINUTE) >= 30 -- Session expiration, as per common rule
        -- Also considering traffic source change
        OR utm_source IS NOT NULL
        OR utm_medium IS NOT NULL
        OR REGEXP_EXTRACT(url, r"[\?&]gclid=([^&]*)") IS NOT NULL
        OR REGEXP_EXTRACT(url, r"[\?&]fbclid=([^&]*)") IS NOT NULL
    )
    {% if is_incremental() %} -- Removing cross day session from day before date range
    AND DATE(event_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()
    {% endif %}
