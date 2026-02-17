SELECT
    form_id,
    form_name,
    form_type
FROM
    {{ ref('stg_airbyte_hubspot_forms') }}
