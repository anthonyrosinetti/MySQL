SELECT
    id AS form_id,
    name AS form_name,
    formType AS form_type
FROM
    {{ source('bronze_airbyte_hubspot', 'forms') }}
