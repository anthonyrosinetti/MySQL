WITH touchpoints AS (
    SELECT
        *
    FROM
        {{ ref('fct_contact_marketing_touchpoints') }}
),
contacts AS (
    SELECT
        hs_contact_id AS contact_id,
        dougs_user_id,
        associated_company_id,
        gender,
        age,
        eligible,
        original_source,
        original_source_drill_down_1,
        first_conversion_form,
        first_conversion_form_type,       
        first_conversion_form_category,
        pack_choice,
        contact_category,
        date_lost,
        date_lead,
        date_mql,
        date_opportunity,
        date_won_invoicing,
        date_first_accounting_subscription
    FROM
        {{ ref('dim_hubspot_contacts') }}
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY hs_contact_id
            ORDER BY date_last_refresh DESC
        ) = 1    
),
company_attributes AS (
    SELECT
        company_id,
        dougs_company_id,
        company_name,
        legal_form,
        ape_code,
        activity,
        subscription_plan,
        quote_1_created_date,
        quote_2_accepted_date,
        lost_reason
    FROM
        {{ ref('dim_hubspot_companies') }}
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY company_id
            ORDER BY date_last_refresh DESC
        ) = 1          
),
attributed_stages AS (
    SELECT DISTINCT
        c.contact_id,
        FIRST_VALUE(t.touchpoint_timestamp IGNORE NULLS) OVER w AS first_touchpoint_timestamp,
        FIRST_VALUE(t.utm_source IGNORE NULLS) OVER w AS first_touchpoint_source,
        FIRST_VALUE(t.utm_medium IGNORE NULLS) OVER w AS first_touchpoint_medium,
        FIRST_VALUE(t.utm_campaign IGNORE NULLS) OVER w AS first_touchpoint_campaign,
        FIRST_VALUE(t.partner_referral_id IGNORE NULLS) OVER w AS first_partner_referral_id,
        FIRST_VALUE(CASE WHEN web_event_type = "form_submission" THEN t.touchpoint_timestamp END IGNORE NULLS) OVER w AS first_form_timestamp,
        FIRST_VALUE(CASE WHEN web_event_type = "form_submission" THEN t.utm_source END IGNORE NULLS) OVER w AS first_form_source,
        FIRST_VALUE(CASE WHEN web_event_type = "form_submission" THEN t.utm_medium END IGNORE NULLS) OVER w AS first_form_medium,
        FIRST_VALUE(CASE WHEN web_event_type = "form_submission" THEN t.utm_campaign END IGNORE NULLS) OVER w AS first_form_campaign
    FROM
        contacts c
    LEFT JOIN
        touchpoints t
    ON
        t.contact_id = c.contact_id
    WINDOW w AS (
        PARTITION BY c.contact_id
        ORDER BY t.touchpoint_timestamp ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
),

attributed_stages_by_first_touchpoint AS (
    SELECT
        contact_id,
        touchpoint_timestamp AS first_touchpoint_timestamp,
        utm_source AS first_touchpoint_source,
        utm_medium AS first_touchpoint_medium,
        utm_campaign AS first_touchpoint_campaign,
        partner_referral_id AS first_touchpoint_partner_referral_id
    FROM
        touchpoints
    WHERE
        (
        utm_source IS NOT NULL
        OR
        utm_medium IS NOT NULL
        OR
        partner_referral_id IS NOT NULL        
        )    
    QUALIFY
        ROW_NUMBER() OVER (
        PARTITION BY contact_id
        ORDER BY touchpoint_timestamp ASC
    ) = 1
),

attributed_stages_by_first_form AS (
    SELECT
        contact_id,
        touchpoint_timestamp AS first_form_timestamp,
        utm_source AS first_form_source,
        utm_medium AS first_form_medium,
        utm_campaign AS first_form_campaign,
        partner_referral_id AS first_form_partner_referral_id
    FROM
        touchpoints
    WHERE
        web_event_type = "form_submission"
        AND
        (
        utm_source IS NOT NULL
        OR
        utm_medium IS NOT NULL
        OR
        partner_referral_id IS NOT NULL
        )
    QUALIFY
        ROW_NUMBER() OVER (
        PARTITION BY contact_id
        ORDER BY touchpoint_timestamp ASC
    ) = 1
),

final_layer AS (
    SELECT
        c.contact_id,
        c.dougs_user_id,
        a.company_id,
        a.dougs_company_id,
        c.gender,
        c.age,
        c.eligible,
        a.legal_form,
        a.ape_code,
        a.activity,
        c.first_conversion_form,
        c.first_conversion_form_category,
        c.first_conversion_form_type,
        c.original_source,
        c.original_source_drill_down_1,
        c.contact_category,
        a.lost_reason,
        CASE
            WHEN c.date_lead IS NULL AND c.date_mql IS NOT NULL THEN 'Direct'
            ELSE 'Indirect'
        END AS contact_type,
        CASE
            WHEN DATE_DIFF(COALESCE(a.quote_2_accepted_date,c.date_first_accounting_subscription), DATE(c.date_lead),DAY) < 7 THEN 'Sec'
            WHEN DATE_DIFF(COALESCE(a.quote_2_accepted_date,c.date_first_accounting_subscription), DATE(c.date_lead),DAY) >= 7 THEN 'Frigo'
            WHEN date_lost < LEAST(c.date_won_invoicing,a.quote_2_accepted_date,c.date_first_accounting_subscription) THEN 'Congel'
            ELSE '-'
        END AS treatment_type,
        first_touchpoint_timestamp,
        first_touchpoint_source,
        first_touchpoint_medium,
        first_touchpoint_campaign,
        first_touchpoint_partner_referral_id,
        first_form_timestamp,
        first_form_source,
        first_form_medium,
        first_form_campaign,
        first_form_partner_referral_id,
        c.date_lost,
        c.date_lead,
        c.date_mql,
        a.quote_1_created_date AS date_opportunity,
        c.date_won_invoicing,
        a.quote_2_accepted_date AS date_won_creation,
        c.date_first_accounting_subscription AS date_won_accounting,
        a.subscription_plan,
        c.pack_choice
    FROM
        contacts c
    LEFT JOIN
        attributed_stages_by_first_touchpoint t
    USING
        (contact_id)
    LEFT JOIN
        attributed_stages_by_first_form f
    USING
        (contact_id)
    LEFT JOIN
        company_attributes a
    ON
        a.company_id = CAST(c.associated_company_id AS STRING)
)
SELECT
    *
FROM
    final_layer
