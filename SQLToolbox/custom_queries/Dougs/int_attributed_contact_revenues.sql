WITH paid_prestations AS (
    SELECT DISTINCT
        company_id,
        DATE(invoice_issued_at) AS date,
        prestation_name_standard,
        prestation_amount,
        prestation_category,
        data_prestation_category
    FROM
        {{ ref('int_prestations') }}
    WHERE
        prestation_is_paid
        AND (
            data_prestation_category = "Ponctuel"
            OR prestation_category IN (
                "subscription",
                "discount"
            )
        )
),
contact_stages AS (
    SELECT
        contact_id,
        dougs_user_id,
        company_id,
        dougs_company_id,
        gender,
        age_range,
        eligible,
        legal_form,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        IFNULL(contact_first_page,"-") AS first_page,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        first_touchpoint_timestamp,
        CASE
            WHEN first_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_touchpoint.source_name) OVER (w_touchpoint_source_medium),"Unmapped")
        END AS first_touchpoint_source_name,
        CASE
            WHEN first_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_touchpoint.channel) OVER (w_touchpoint_source_medium),"Unmapped")
        END AS first_touchpoint_channel,
        CASE
            WHEN first_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(camp_touchpoint.campaign_group) OVER (w_touchpoint_campaign),"Unmapped")
        END AS first_touchpoint_campaign_group,
        IFNULL(first_touchpoint_term,"No keyword") AS first_touchpoint_term,
--        IFNULL(first_touchpoint_page,"-") AS first_touchpoint_page,
        first_form_timestamp,
        CASE
            WHEN first_form_timestamp IS NULL THEN "No touchpoint"
            WHEN first_form_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_form.source_name) OVER (w_form_source_medium),"Unmapped")
        END AS first_form_source_name,
        CASE
            WHEN first_form_timestamp IS NULL THEN "No touchpoint"
            WHEN first_form_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_form.channel) OVER (w_form_source_medium),"Unmapped")
        END AS first_form_channel,        
        CASE
            WHEN first_form_timestamp IS NULL THEN "No touchpoint"
            WHEN first_form_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(camp_form.campaign_group) OVER (w_form_campaign),"Unmapped")
        END AS first_form_campaign_group,
        IFNULL(first_form_term,"No keyword") AS first_form_term,
--        IFNULL(first_form_page,"-") AS first_form_page,
        date_lost,
        date_lead,
        date_mql,
        date_sql,
        date_opportunity,
        date_signup_invoicing,
        date_won_invoicing,
        date_won_creation,
        date_won_accounting,
        pack_choice
    FROM
        {{ ref('fct_contact_stages_attributions') }} c
    LEFT JOIN
        {{ ref('dim_mapping_source_medium') }} s_touchpoint
    ON
        (s_touchpoint.utm_source IS NULL OR LOWER(c.first_touchpoint_source) LIKE LOWER(s_touchpoint.utm_source))
        AND
        (s_touchpoint.utm_medium IS NULL OR LOWER(c.first_touchpoint_medium) LIKE LOWER(s_touchpoint.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_source_medium') }} s_form
    ON
        (s_form.utm_source IS NULL OR LOWER(c.first_form_source) LIKE LOWER(s_form.utm_source))
        AND
        (s_form.utm_medium IS NULL OR LOWER(c.first_form_medium) LIKE LOWER(s_form.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} camp_touchpoint
    ON
        (camp_touchpoint.utm_campaign IS NULL OR LOWER(c.first_touchpoint_campaign) LIKE LOWER(camp_touchpoint.utm_campaign))
        AND
        (camp_touchpoint.utm_source IS NULL OR LOWER(c.first_touchpoint_source) LIKE LOWER(camp_touchpoint.utm_source))
        AND
        (camp_touchpoint.utm_medium IS NULL OR LOWER(c.first_touchpoint_medium) LIKE LOWER(camp_touchpoint.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} camp_form
    ON
        (camp_form.utm_campaign IS NULL OR LOWER(c.first_touchpoint_campaign) LIKE LOWER(camp_form.utm_campaign))
        AND
        (camp_form.utm_source IS NULL OR LOWER(c.first_touchpoint_source) LIKE LOWER(camp_form.utm_source))
        AND
        (camp_form.utm_medium IS NULL OR LOWER(c.first_touchpoint_medium) LIKE LOWER(camp_form.utm_medium))
    WINDOW
        w_touchpoint_source_medium AS (
            PARTITION BY s_touchpoint.utm_source, s_touchpoint.utm_medium
            ORDER BY s_touchpoint.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_form_source_medium AS (
            PARTITION BY s_form.utm_source, s_form.utm_medium
            ORDER BY s_form.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),   
        w_touchpoint_campaign AS (
            PARTITION BY camp_touchpoint.utm_campaign, camp_touchpoint.utm_source, camp_touchpoint.utm_medium
            ORDER BY camp_touchpoint.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_form_campaign AS (
            PARTITION BY camp_form.utm_campaign, camp_form.utm_source, camp_form.utm_medium
            ORDER BY camp_form.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
)

SELECT
    c.*,
    CAST(NULL AS STRING) AS event_type,
    CAST(NULL AS DATE) AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
FROM
    contact_stages c

UNION ALL

SELECT
    c.*,
    "prestation" AS event_type,
    date AS event_date,
    prestation_name_standard AS prestation_name,
    prestation_category,
    data_prestation_category,
    ROUND(prestation_amount,2) AS prestation_amount
FROM
    paid_prestations p
LEFT JOIN
    contact_stages c
ON
    c.dougs_company_id = p.company_id

UNION ALL

SELECT
    c.*,
    "lost" AS event_type,
    date_lost AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
FROM
    contact_stages c
WHERE
    date_lost IS NOT NULL

UNION ALL

SELECT
    c.*,
    "lead" AS event_type,
    date_lead AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
FROM
    contact_stages c
WHERE
    date_lead IS NOT NULL

UNION ALL

SELECT
    c.*,
    "mql" AS event_type,
    date_mql AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
FROM
    contact_stages c
WHERE
    date_mql IS NOT NULL

UNION ALL

SELECT
    c.*,
    "sql" AS event_type,
    date_sql AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
FROM
    contact_stages c
WHERE
    date_sql IS NOT NULL

UNION ALL

SELECT
    c.*,
    "opportunity" AS event_type,
    date_opportunity AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
FROM
    contact_stages c
WHERE
    date_opportunity IS NOT NULL

UNION ALL

SELECT
    c.*,
    "won_invoicing" AS event_type,
    date_won_invoicing AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
FROM
    contact_stages c
WHERE
    date_won_invoicing IS NOT NULL

UNION ALL

SELECT
    c.*,
    "won_creation" AS event_type,
    date_won_creation AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
FROM
    contact_stages c
WHERE
    date_won_creation IS NOT NULL

UNION ALL

SELECT
    c.*,
    "won_accounting" AS event_type,
    date_won_accounting AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
FROM
    contact_stages c
WHERE
    date_won_accounting IS NOT NULL
