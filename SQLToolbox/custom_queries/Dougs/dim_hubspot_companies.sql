
WITH classification_info_per_company AS (
    SELECT
        comp.company_id,
        MAX(clas.id_2) AS ape_code_2d,
        MAX(clas.id_label_2d) AS id_label_2d
    FROM
        {{ ref('dim_companies') }} AS comp
    INNER JOIN
        {{ ref('dim_classification_naf') }} AS clas
    ON
        comp.ape_code_2d = clas.id_2
    GROUP BY
        comp.company_id
)

SELECT
    comp.company_id,
    comp.dougs_company_id,
    comp.company_name,
    comp.contact_1,
    comp.contact_2,
    comp.contact_3,
    comp.created_at,
    comp.updated_at,
    cc.country_fr_name AS country,
    comp.legal_form,
    comp.ape_code,
    comp.activity,
    comp.lead_status,
    comp.has_company_creation,
    comp.subscription_plan,
    comp.eligible,
    comp.quote_1_created_date,
    comp.quote_2_accepted_date,
    comp.lost_reason,
    comp.date_last_refresh,
    CASE
        WHEN clas.id_label_2d IS NOT NULL THEN clas.id_label_2d
        WHEN comp.ape_code IS NOT NULL THEN 'Non reconnu'
        ELSE '-'
    END AS ape_activity_name
FROM
    {{ ref('stg_airbyte_hubspot_companies') }} AS comp
LEFT JOIN
    classification_info_per_company AS clas
ON
    comp.dougs_company_id = clas.company_id
LEFT JOIN
    {{ ref('country_codes') }} AS cc
ON
    cc.country_en_name = comp.country
