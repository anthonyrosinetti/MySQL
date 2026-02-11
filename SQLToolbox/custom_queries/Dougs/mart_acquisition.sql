{{ config(
    materialized="table",
    partition_by={
        "field": "TIMESTAMP_TRUNC(touchpoint_timestamp, DAY)",
        "data_type": "date",
        "granularity": "day"
    }
) }}

WITH touchpoints AS (
    SELECT
        t.*
    FROM
        {{ ref('fct_contact_marketing_touchpoints') }} t
    LEFT JOIN
        {{ ref('touchpoints_ignored') }} i
    ON
        i.utm_source = t.utm_source
        AND
        i.utm_medium = t.utm_medium
        AND
        i.utm_campaign = t.utm_campaign
    WHERE
        i.utm_source IS NULL
        AND
        i.utm_medium IS NULL
        AND
        i.utm_campaign IS NULL       
),
contacts AS (
    SELECT DISTINCT
        hs_contact_id AS contact_id,
        dougs_user_id,
        associated_company_id,
        gender,
        age_range,
        eligible,
        lead_source,
        original_source,
        original_source_drill_down_1,
        first_conversion_form,
        first_conversion_form_type,       
        first_conversion_form_category,
        contact_category,
        date_lost,
        date_lead,
        date_mql,
        date_sql,
        date_opportunity,
        date_signup_invoicing,
        date_won_invoicing,
        date_won_accounting,
        date_won_creation,
        date_first_subscription_activated,
        first_subscription_plan AS pack_choice
    FROM
        {{ ref('dim_hubspot_contacts') }}
),
company_attributes AS (
    SELECT DISTINCT
        company_id,
        dougs_company_id,
        company_name,
        country,
        legal_form,
        IFNULL(ape_activity_name, '-') AS ape_activity_name,
        activity,
        quote_1_created_date,
        quote_2_accepted_date,
        lost_reason
    FROM
        {{ ref('dim_hubspot_companies') }}      
),
enriched_touchpoints AS (
    SELECT DISTINCT
        t.* EXCEPT (contact_id, contact_first_page),
        ROW_NUMBER() OVER (PARTITION BY contact_id ORDER BY touchpoint_timestamp ASC) AS touchpoint_row_number,
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
        IFNULL(CASE
            WHEN c.first_conversion_form IN ('Signup creation','Signup accounting','contact','RDV Sales Calendly','Other Calendly meetings','Intercom') THEN 'MQL First'
            ELSE 'Lead First'
        END, '-') AS contact_type,
        IFNULL(CASE
            WHEN date_lost < LEAST(c.date_won_invoicing,ca.quote_2_accepted_date,c.date_first_subscription_activated) THEN 'Congel'
            WHEN DATE_DIFF(COALESCE(ca.quote_2_accepted_date,c.date_first_subscription_activated), DATE(c.date_lead),DAY) <= 7 THEN 'Sec'
            WHEN DATE_DIFF(COALESCE(ca.quote_2_accepted_date,c.date_first_subscription_activated), DATE(c.date_lead),DAY) > 7 THEN 'Frigo'
            ELSE '-'
        END, '-') AS treatment_type,     
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
        contacts c
    LEFT JOIN
        touchpoints t
    USING
        (contact_id)
    LEFT JOIN
        company_attributes ca
    ON
        ca.company_id = CAST(c.associated_company_id AS STRING)
),
final_layer AS (
    SELECT
        e.* EXCEPT (touchpoint_row_number),
        CASE
            WHEN touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s.source_name) OVER (w_source_medium),"Unmapped")
        END AS source_name,
        CASE
            WHEN touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s.channel) OVER (w_source_medium),"Unmapped")
        END AS channel,        
        CASE
            WHEN touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(c.campaign_group) OVER (w_campaign),"Unmapped")
        END AS campaign_group,
        IFNULL(utm_term,"No keyword") AS term,        
        CASE WHEN e.touchpoint_row_number = 1 THEN True ELSE False END AS is_first_touch_attribution_model,
        CASE WHEN e.touchpoint_id = FIRST_VALUE(CASE WHEN web_event_type = "form_submission" THEN e.touchpoint_id ELSE NULL END)
            OVER (PARTITION BY contact_id ORDER BY DATE(e.touchpoint_timestamp) ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
            THEN True ELSE False END AS is_first_form_attribution_model,
        CASE WHEN e.touchpoint_id = FIRST_VALUE(CASE WHEN DATE(e.touchpoint_timestamp) <= date_lead THEN e.touchpoint_id ELSE NULL END)
            OVER (PARTITION BY contact_id ORDER BY CASE WHEN DATE(e.touchpoint_timestamp) <= date_lead THEN 0 ELSE 1 END ASC, DATE(e.touchpoint_timestamp) DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN True ELSE False END AS is_lead_last_touch_attribution_model,
        CASE WHEN e.touchpoint_id = FIRST_VALUE(CASE WHEN DATE(e.touchpoint_timestamp) <= date_mql THEN e.touchpoint_id ELSE NULL END)
            OVER (PARTITION BY contact_id ORDER BY CASE WHEN DATE(e.touchpoint_timestamp) <= date_mql THEN 0 ELSE 1 END ASC, DATE(e.touchpoint_timestamp) DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) THEN True ELSE False END AS is_mql_last_touch_attribution_model,
        CASE WHEN DATE(e.touchpoint_timestamp) <= date_lead THEN True ELSE False END AS is_lead_linear_attribution_model,
        CASE WHEN DATE(e.touchpoint_timestamp) <= date_mql THEN True ELSE False END AS is_mql_linear_attribution_model
    FROM
        enriched_touchpoints e
    LEFT JOIN
        {{ ref('dim_mapping_source_medium') }} s
    ON
        (s.utm_source IS NULL OR LOWER(e.utm_source) LIKE LOWER(s.utm_source))
        AND
        (s.utm_medium IS NULL OR LOWER(e.utm_medium) LIKE LOWER(s.utm_medium))    
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} c
    ON
        (c.utm_campaign IS NULL OR LOWER(e.utm_campaign) LIKE LOWER(c.utm_campaign))
        AND
        (c.utm_source IS NULL OR LOWER(e.utm_source) LIKE LOWER(c.utm_source))
        AND
        (c.utm_medium IS NULL OR LOWER(e.utm_medium) LIKE LOWER(c.utm_medium))
    WINDOW
        w_source_medium AS (
            PARTITION BY s.utm_source, s.utm_medium
            ORDER BY s.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_campaign AS (
            PARTITION BY c.utm_campaign, c.utm_source, c.utm_medium
            ORDER BY c.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )        
)
SELECT
    *,
    CAST(NULL AS STRING) AS model_type
FROM
    final_layer
