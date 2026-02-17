SELECT
    id AS hs_event_id,
    objectId AS hs_contact_id,
    TIMESTAMP(occurredAt) AS event_timestamp,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.form_id")), "") AS form_id,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.form_type")), "") AS form_type,
    NULLIF(LAX_STRING(JSON_QUERY(properties, "$.visitor_type")), "") AS visitor_type
FROM
    {{ source('bronze_airbyte_hubspot', 'e_form_submission_v2') }}
