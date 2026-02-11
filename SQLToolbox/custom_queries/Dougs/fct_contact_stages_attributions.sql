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
        t.contact_first_page,
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
        first_subscription_plan
    FROM
        {{ ref('dim_hubspot_contacts') }} c
        LEFT JOIN
            touchpoints t
        ON
            t.contact_id = c.hs_contact_id
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
        {{ ref('dim_hubspot_companies') }} c         
),
touchpoints_with_dates AS (
    SELECT DISTINCT
        t.*,
        c.date_lead,
        c.date_mql,
        c.date_won_accounting,
        c.date_won_creation
    FROM
        touchpoints t
        LEFT JOIN
            contacts c
        USING
            (contact_id)           
),
attributed_stages_by_first_touchpoint_all AS (
    SELECT DISTINCT
        contact_id,
        touchpoint_timestamp AS first_touchpoint_timestamp,
        utm_source AS first_touchpoint_source,
        utm_medium AS first_touchpoint_medium,
        utm_campaign AS first_touchpoint_campaign,
        utm_term AS first_touchpoint_term,
--        REGEXP_EXTRACT(landing_page, r"^(?:https?://)?[^/]+(/[^?#]*)") AS first_touchpoint_page,
        partner_referral_id AS first_touchpoint_partner_referral_id
    FROM
        touchpoints   
    QUALIFY
        ROW_NUMBER() OVER (
        PARTITION BY contact_id
        ORDER BY touchpoint_timestamp ASC
    ) = 1
),
attributed_stages_by_first_touchpoint AS (
    SELECT DISTINCT
        contact_id,
        touchpoint_timestamp AS first_touchpoint_timestamp,
        utm_source AS first_touchpoint_source,
        utm_medium AS first_touchpoint_medium,
        utm_campaign AS first_touchpoint_campaign,
        utm_term AS first_touchpoint_term,
--        REGEXP_EXTRACT(landing_page, r"^(?:https?://)?[^/]+(/[^?#]*)") AS first_touchpoint_page,
        partner_referral_id AS first_touchpoint_partner_referral_id
    FROM
        touchpoints
    WHERE
        utm_source IS NOT NULL
        OR utm_medium IS NOT NULL
        OR partner_referral_id IS NOT NULL
    QUALIFY
        ROW_NUMBER() OVER (
        PARTITION BY contact_id
        ORDER BY touchpoint_timestamp ASC
    ) = 1
),
attributed_stages_by_first_form_all AS (
    SELECT DISTINCT
        contact_id,
        touchpoint_timestamp AS first_form_timestamp,
        utm_source AS first_form_source,
        utm_medium AS first_form_medium,
        utm_campaign AS first_form_campaign,
        utm_term AS first_form_term,
--        REGEXP_EXTRACT(landing_page, r"^(?:https?://)?[^/]+(/[^?#]*)") AS first_form_page,
        partner_referral_id AS first_form_partner_referral_id
    FROM
        touchpoints
    WHERE
        web_event_type = "form_submission"
    QUALIFY
        ROW_NUMBER() OVER (
        PARTITION BY contact_id
        ORDER BY touchpoint_timestamp ASC
    ) = 1
),
attributed_stages_by_first_form AS (
    SELECT DISTINCT
        contact_id,
        touchpoint_timestamp AS first_form_timestamp,
        utm_source AS first_form_source,
        utm_medium AS first_form_medium,
        utm_campaign AS first_form_campaign,
        utm_term AS first_form_term,
--        REGEXP_EXTRACT(landing_page, r"^(?:https?://)?[^/]+(/[^?#]*)") AS first_form_page,
        partner_referral_id AS first_form_partner_referral_id
    FROM
        touchpoints
    WHERE
        web_event_type = "form_submission"
        AND (
            utm_source IS NOT NULL
            OR utm_medium IS NOT NULL
            OR partner_referral_id IS NOT NULL
        )
    QUALIFY
        ROW_NUMBER() OVER (
        PARTITION BY contact_id
        ORDER BY touchpoint_timestamp ASC
    ) = 1
),
attributed_lead_by_last_touchpoint AS (
    SELECT DISTINCT
        contact_id,
        LAST_VALUE(touchpoint_timestamp) OVER w AS lead_last_touchpoint_timestamp,
        LAST_VALUE(utm_source) OVER w AS lead_last_touchpoint_source,
        LAST_VALUE(utm_medium) OVER w AS lead_last_touchpoint_medium,
        LAST_VALUE(utm_campaign) OVER w AS lead_last_touchpoint_campaign,
        LAST_VALUE(utm_term) OVER w AS lead_last_touchpoint_term,
        LAST_VALUE(partner_referral_id) OVER w AS lead_last_touchpoint_partner_referral_id
    FROM
        touchpoints_with_dates
    WHERE
        DATE(touchpoint_timestamp) <= date_lead
    WINDOW w AS (
            PARTITION BY contact_id
            ORDER BY touchpoint_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),
attributed_mql_by_last_touchpoint AS (
    SELECT DISTINCT
        contact_id,
        LAST_VALUE(touchpoint_timestamp) OVER w AS mql_last_touchpoint_timestamp,
        LAST_VALUE(utm_source) OVER w AS mql_last_touchpoint_source,
        LAST_VALUE(utm_medium) OVER w AS mql_last_touchpoint_medium,
        LAST_VALUE(utm_campaign) OVER w AS mql_last_touchpoint_campaign,
        LAST_VALUE(utm_term) OVER w AS mql_last_touchpoint_term,
        LAST_VALUE(partner_referral_id) OVER w AS mql_last_touchpoint_partner_referral_id
    FROM
        touchpoints_with_dates
    WHERE
        DATE(touchpoint_timestamp) <= date_mql        
    WINDOW w AS (
            PARTITION BY contact_id
            ORDER BY touchpoint_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),
lead_touchpoints AS (
    SELECT DISTINCT
        contact_id,
        COUNT(DISTINCT touchpoint_id) AS touchpoints_count
    FROM
        touchpoints_with_dates
    WHERE
        DATE(touchpoint_timestamp) <= date_lead
        AND
        contact_id IS NOT NULL
    GROUP BY
      contact_id
),
mql_touchpoints AS (
    SELECT DISTINCT
        contact_id,
        COUNT(DISTINCT touchpoint_id) AS touchpoints_count
    FROM
        touchpoints_with_dates
    WHERE
        DATE(touchpoint_timestamp) <= date_mql
        AND
        contact_id IS NOT NULL        
    GROUP BY
      contact_id
),
pre_conversion_touchpoints AS (
    SELECT DISTINCT
        contact_id,
        COUNT(DISTINCT touchpoint_id) AS pre_conversion_touchpoints,
        COUNT(DISTINCT CASE WHEN web_event_type = "form_submission" THEN touchpoint_id ELSE NULL END) AS pre_conversion_forms
    FROM
        touchpoints_with_dates      
    WHERE
        DATE(touchpoint_timestamp) <= LEAST(date_won_accounting,date_won_creation)
        AND
        contact_id IS NOT NULL        
    GROUP BY
      contact_id    
),
attributed_lead_linear AS (
    SELECT DISTINCT
        contact_id,
        ARRAY_AGG(
            STRUCT(
                touchpoint_timestamp,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_term,
                partner_referral_id,
                SAFE_DIVIDE(1,touchpoints_count) AS weight
            )
        ) AS lead_linear_touchpoint
    FROM
        touchpoints_with_dates
        LEFT JOIN
          lead_touchpoints
        USING (contact_id)
    WHERE
        DATE(touchpoint_timestamp) <= date_lead
    GROUP BY
      contact_id
),
attributed_mql_linear AS (
    SELECT DISTINCT
        contact_id,
        ARRAY_AGG(
            STRUCT(
                touchpoint_timestamp,
                utm_source,
                utm_medium,
                utm_campaign,
                utm_term,
                partner_referral_id,
                SAFE_DIVIDE(1,touchpoints_count) AS weight
            )
        ) AS mql_linear_touchpoint
    FROM
        touchpoints_with_dates
        LEFT JOIN
          mql_touchpoints
        USING (contact_id)        
    WHERE
        DATE(touchpoint_timestamp) <= date_mql
    GROUP BY
      contact_id        
)

SELECT
    c.contact_id,
    c.dougs_user_id,
    a.company_id,
    a.dougs_company_id,
    c.gender,
    c.age_range,
    c.eligible,
    a.legal_form,
    a.country,
    a.ape_activity_name,
    a.activity,
    c.first_conversion_form,
    c.first_conversion_form_category,
    c.first_conversion_form_type,
    c.contact_first_page,
    c.lead_source,
    c.original_source,
    c.original_source_drill_down_1,
    c.contact_category,
    a.lost_reason,
    IFNULL(CASE
        WHEN c.first_conversion_form IN ('Signup creation','Signup accounting','contact','RDV Sales Calendly','Other Calendly meetings','Intercom') THEN 'MQL First'
        ELSE 'Lead First'
    END, '-') AS contact_type,
    IFNULL(CASE
        WHEN date_lost < LEAST(c.date_won_invoicing,a.quote_2_accepted_date,c.date_first_subscription_activated) THEN 'Congel'
        WHEN DATE_DIFF(COALESCE(a.quote_2_accepted_date,c.date_first_subscription_activated), DATE(c.date_lead),DAY) <= 7 THEN 'Sec'
        WHEN DATE_DIFF(COALESCE(a.quote_2_accepted_date,c.date_first_subscription_activated), DATE(c.date_lead),DAY) > 7 THEN 'Frigo'
        ELSE '-'
    END, '-') AS treatment_type,
    CASE
        WHEN t.first_touchpoint_timestamp IS NOT NULL THEN t.first_touchpoint_timestamp
        ELSE tall.first_touchpoint_timestamp
    END AS first_touchpoint_timestamp,
    CASE
        WHEN t.first_touchpoint_timestamp IS NOT NULL THEN t.first_touchpoint_source
        ELSE tall.first_touchpoint_source
    END AS first_touchpoint_source,
    CASE
        WHEN t.first_touchpoint_timestamp IS NOT NULL THEN t.first_touchpoint_medium
        ELSE tall.first_touchpoint_medium
    END AS first_touchpoint_medium,
    CASE
        WHEN t.first_touchpoint_timestamp IS NOT NULL THEN t.first_touchpoint_campaign
        ELSE tall.first_touchpoint_campaign
    END AS first_touchpoint_campaign,
    CASE
        WHEN t.first_touchpoint_term IS NOT NULL THEN t.first_touchpoint_term
        ELSE tall.first_touchpoint_term
    END AS first_touchpoint_term, 
    CASE
        WHEN t.first_touchpoint_timestamp IS NOT NULL THEN t.first_touchpoint_partner_referral_id
        ELSE tall.first_touchpoint_partner_referral_id
    END AS first_touchpoint_partner_referral_id,
    CASE
        WHEN f.first_form_timestamp IS NOT NULL THEN f.first_form_timestamp
        ELSE fall.first_form_timestamp
    END AS first_form_timestamp,
    CASE
        WHEN f.first_form_timestamp IS NOT NULL THEN f.first_form_source
        ELSE fall.first_form_source
    END AS first_form_source,
    CASE
        WHEN f.first_form_timestamp IS NOT NULL THEN f.first_form_medium
        ELSE fall.first_form_medium
    END AS first_form_medium,
    CASE
        WHEN f.first_form_timestamp IS NOT NULL THEN f.first_form_campaign
        ELSE fall.first_form_campaign
    END AS first_form_campaign,
    CASE
        WHEN f.first_form_term IS NOT NULL THEN f.first_form_term
        ELSE fall.first_form_term
    END AS first_form_term, 
    CASE
        WHEN f.first_form_timestamp IS NOT NULL THEN f.first_form_partner_referral_id
        ELSE fall.first_form_partner_referral_id
    END AS first_form_partner_referral_id,
    lead_last_touchpoint_timestamp,
    lead_last_touchpoint_source,
    lead_last_touchpoint_medium,
    lead_last_touchpoint_campaign,
    lead_last_touchpoint_term, 
    lead_last_touchpoint_partner_referral_id,
    mql_last_touchpoint_timestamp,
    mql_last_touchpoint_source,
    mql_last_touchpoint_medium,
    mql_last_touchpoint_campaign,
    mql_last_touchpoint_term, 
    mql_last_touchpoint_partner_referral_id,
    lead_linear_touchpoint,
    mql_linear_touchpoint,
    p.pre_conversion_touchpoints,
    p.pre_conversion_forms,
    c.date_lost,
    c.date_lead,
    c.date_mql,
    c.date_sql,
    -- a.quote_1_created_date AS date_opportunity,
    c.date_opportunity,
    c.date_signup_invoicing,
    c.date_won_invoicing,
    -- a.quote_2_accepted_date AS date_won_creation,
    c.date_won_creation,
    c.date_won_accounting,
    -- c.date_first_subscription_activated AS date_won_accounting,
    c.first_subscription_plan AS pack_choice
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
    attributed_lead_by_last_touchpoint l
USING
    (contact_id)
LEFT JOIN
    attributed_mql_by_last_touchpoint m
USING
    (contact_id)
LEFT JOIN
    attributed_stages_by_first_touchpoint_all tall
USING
    (contact_id)
LEFT JOIN
    attributed_stages_by_first_form_all fall
USING
    (contact_id)
LEFT JOIN
    attributed_lead_linear ll
USING
    (contact_id)
LEFT JOIN
    attributed_mql_linear ml
USING
    (contact_id)
LEFT JOIN
    pre_conversion_touchpoints p
USING
    (contact_id)
LEFT JOIN
    company_attributes a
ON
    a.company_id = CAST(c.associated_company_id AS STRING)
