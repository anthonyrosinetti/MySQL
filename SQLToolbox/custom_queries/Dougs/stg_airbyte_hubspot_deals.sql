SELECT
    CAST(sahd.id AS STRING) AS deal_id,
    NULLIF(REPLACE(TO_JSON_STRING(sahd.contacts[0]) ,"\"", ""), 'null') AS contact_1,
    NULLIF(REPLACE(TO_JSON_STRING(sahd.contacts[1]) ,"\"", ""), 'null') AS contact_2,
    NULLIF(REPLACE(TO_JSON_STRING(sahd.contacts[2]) ,"\"", ""), 'null') AS contact_3,
    NULLIF(REPLACE(TO_JSON_STRING(sahd.companies[0]) ,"\"", ""), 'null') AS company_1,
    NULLIF(REPLACE(TO_JSON_STRING(sahd.companies[1]) ,"\"", ""), 'null') AS company_2,
    NULLIF(REPLACE(TO_JSON_STRING(sahd.companies[2]) ,"\"", ""), 'null') AS company_3,
    EXTRACT(DATE FROM sahd.createdAt) AS created_at,
    EXTRACT(DATE FROM sahd.updatedAt) AS updated_at,
    sahd.properties_amount AS amount,
    EXTRACT(DATE FROM sahd.properties_closedate) AS close_date,
    sahd.properties_dealstage AS deal_stage,
    sahd.properties_raison_de_la_perte,
    sahd.properties_raison_de_rejet___ AS properties_raison_de_rejet,
    sahd.properties_last_conversion_form,
    sahd.properties_createdate,
    _airbyte_extracted_at AS date_last_refresh
FROM {{ source('bronze_airbyte_hubspot', 'deals') }} AS sahd
