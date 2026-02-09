WITH prestation_info_per_company AS (
    SELECT
        p.company_id,
        MIN(CASE WHEN p.data_prestation_category = 'Abonnements' AND department = 'accounting' THEN p.invoice_issued_at END) AS date_first_accounting_subscription,
        MAX(CASE WHEN p.data_prestation_category = 'Abonnements' THEN p.invoice_issued_at END) AS date_last_subscription_prestation_billed  --churned_at
    FROM
        {{ ref('fct_prestations') }} p
    GROUP BY
        p.company_id 
),
subscription_info_per_company AS (
    SELECT DISTINCT
        ss.company_id, 
        FIRST_VALUE(ss.is_annually_paid) OVER (PARTITION BY ss.company_id ORDER BY ss.dbt_valid_from DESC) AS is_annually_paid_last_subscription,
        FIRST_VALUE(ss.activated_at) OVER (PARTITION BY ss.company_id ORDER BY ss.dbt_valid_from ASC) AS date_first_subscription_activated,  --first_activated_at
        FIRST_VALUE(ss.suspended_at) OVER (PARTITION BY ss.company_id ORDER BY ss.dbt_valid_from DESC) AS date_last_subscription_suspended, 
        FIRST_VALUE(ss.plan) OVER (PARTITION BY ss.company_id ORDER BY ss.dbt_valid_from ASC) AS first_subscription_plan,
        FIRST_VALUE(ss.plan) OVER (PARTITION BY ss.company_id ORDER BY ss.dbt_valid_from DESC) AS last_subscription_plan
    FROM
        {{ ref('subscriptions_snapshot') }} ss
    WHERE
        ss.plan IS NOT NULL
        AND ss.activated_at IS NOT NULL
),
churned_at_cte AS (
    SELECT 
        subscription_info_per_company.company_id,
        CASE 
            WHEN subscription_info_per_company.is_annually_paid_last_subscription = TRUE THEN subscription_info_per_company.date_last_subscription_suspended
            WHEN prestation_info_per_company.date_last_subscription_prestation_billed >= subscription_info_per_company.date_last_subscription_suspended THEN prestation_info_per_company.date_last_subscription_prestation_billed
            ELSE subscription_info_per_company.date_last_subscription_suspended
        END AS churned_at 
    FROM 
        {{ ref('dim_companies') }} c
    LEFT JOIN
        prestation_info_per_company 
    ON
        prestation_info_per_company.company_id = c.company_id
    LEFT JOIN
        subscription_info_per_company 
    ON
        subscription_info_per_company.company_id = c.company_id
),
invoicing_signups AS (
    SELECT DISTINCT
        user_id,
        DATE(created_at) AS date_signup_invoicing
    FROM
        {{ ref('dim_users') }}
    WHERE
        is_signup_completed
        AND
        deleted_at IS NULL        
)
SELECT
    c.hs_contact_id,
    c.dougs_user_id,
    c.associated_company_id,
    c.company_1,
    c.company_2,
    c.company_3,
    c.created_at_timestamp,
    c.created_at,
    c.updated_at_timestamp,
    c.updated_at,
    c.owner,
    c.owner_id,
    c.original_source,
    c.original_source_drill_down_1,
    c.first_conversion_date,
    c.first_conversion_form,
    c.first_conversion_form_type,
    c.first_conversion_form_category,
    c.pack_choice,
    c.product_types,
    c.lead_source,
    c.lead_status,
    c.contact_category,
    c.date_lost,
    c.date_lead,
    c.date_mql,
    c.date_sql,
    c.date_opportunity,
    i.date_signup_invoicing,
    c.date_won_invoicing,
    c.date_won_accounting,
    c.date_won_creation,
    c.date_last_refresh,
    np.first_name,
    np.last_name,
    np.email,
    np.phone,    
    IFNULL(CASE
        WHEN np.is_man IS TRUE THEN 'H'
        ELSE 'F'
    END, '-') AS gender,
    {{age_bucket(
            'CASE
                WHEN (ca.churned_at IS NOT NULL AND ca.churned_at <= CURRENT_DATE()) THEN
                    DATE_DIFF(DATE(ca.churned_at), DATE(np.birth_date), YEAR)
                ELSE DATE_DIFF(DATE(CURRENT_DATE()), DATE(np.birth_date), YEAR)
            END'
    )}} AS age_range,
    CASE
        WHEN c.eligible IS NULL THEN True
        WHEN c.eligible = '' THEN True
        ELSE CAST(c.eligible AS BOOL)
    END AS eligible,
    s.date_first_subscription_activated,
    s.first_subscription_plan
FROM
    {{ ref('stg_airbyte_hubspot_contacts') }} c
LEFT JOIN
    {{ ref('dim_users') }} us
ON
    us.user_id = c.dougs_user_id
LEFT JOIN 
    {{ref('dim_natural_people')}} np
ON
    np.natural_person_id = us.user_id
LEFT JOIN
    invoicing_signups i
ON
    i.user_id = c.dougs_user_id
LEFT JOIN
    {{ref('dim_hubspot_companies')}} hc
ON
    hc.company_id = CAST(c.associated_company_id AS STRING)
LEFT JOIN
    churned_at_cte ca
ON
    ca.company_id = hc.dougs_company_id
LEFT JOIN
    subscription_info_per_company s
ON
    s.company_id = hc.dougs_company_id
WHERE
    -- Excluding contacts whose owner is Thomas F. as those are tests
    c.owner_id NOT IN (
        "158401203"
    )
    OR c.owner_id IS NULL
