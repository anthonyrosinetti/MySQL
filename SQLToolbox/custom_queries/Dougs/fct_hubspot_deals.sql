
SELECT
    sahd.deal_id,
    sahd.contact_1,
    sahd.contact_2,
    sahd.contact_3,
    sahd.company_1,
    sahd.company_2,
    sahd.company_3,
    sahd.created_at,
    sahd.updated_at,
    sahd.amount,
    sahd.close_date,
    sahd.deal_stage,
    sahd.properties_raison_de_la_perte,
    sahd.properties_raison_de_rejet,
    sahd.properties_last_conversion_form,
    sahd.properties_createdate,
    date_last_refresh
FROM {{ ref('stg_airbyte_hubspot_deals') }} AS sahd
