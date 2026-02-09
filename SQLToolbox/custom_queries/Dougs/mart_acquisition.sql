WITH final_layer AS (
    SELECT
        contact_id,
        dougs_user_id,
        company_id,
        dougs_company_id,
        gender,
        age_range,
        eligible,
        legal_form,
        country,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        IFNULL(contact_first_page,"-") AS first_page,
        lead_source,
        original_source,
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
    *
FROM
    final_layer
