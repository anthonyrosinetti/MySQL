{{ config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key="hs_event_id"
) }}

SELECT
    id AS hs_event_id,
    objectId AS hs_contact_id,
    TIMESTAMP(occurredAt) AS event_timestamp,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_company_id")), "") AS hs_company_id,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_referrer")), "") AS referrer,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_url")), "") AS url,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_utm_source")), "") AS utm_source,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_utm_medium")), "") AS utm_medium,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_utm_campaign")), "") AS utm_campaign,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_utm_content")), "") AS utm_content,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_utm_term")), "") AS utm_term,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_visit_source")), "") AS visit_source,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_visit_source_details_1")), "") AS visit_source_details_1,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_visit_source_details_2")), "") AS visit_source_details_2,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.hs_browser_fingerprint")), "") AS hs_browser_fingerprint
FROM
    {{ source('bronze_airbyte_hubspot', 'e_visited_page') }}
{% if is_incremental() %}
WHERE
    -- Due to the size of the table, we process data in an incremental strategy, on a 3 day rolling window
    DATE(_airbyte_extracted_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()
{% endif %}
