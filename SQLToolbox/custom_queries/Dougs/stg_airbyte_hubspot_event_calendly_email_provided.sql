SELECT
    id AS hs_event_id,
    objectId AS hs_contact_id,
    TIMESTAMP(occurredAt) AS event_timestamp,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.event_type")), "") AS web_event_type,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_utm_source")), "") AS session_utm_source,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_utm_medium")), "") AS session_utm_medium,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_utm_campaign")), "") AS session_utm_campaign,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_utm_id")), "") AS session_utm_id,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_utm_content")), "") AS session_utm_content,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_utm_term")), "") AS session_utm_term,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_referrer")), "") AS session_referrer,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_landing_page")), "") AS session_landing_page,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_gclid")), "") AS session_gclid,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_fbclid")), "") AS session_fbclid,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.session_ga_visitor_id")), "") AS session_ga_visitor_id,
FROM
    {{ source('bronze_airbyte_hubspot', 'calendly_email_provided') }}
