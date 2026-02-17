SELECT 
    s.subscription_id,
    s.plan,
    CASE
        WHEN (LOWER(s.plan) IN ('social')) THEN 'Social'
        WHEN (LOWER(s.plan) IN (
                'liberté',
                'opportunité',
                'comptastart',
                'sérénité',
                'sci',
                'micro',
                'lmnp',
                'exclusivité',
                'lmp',
                'essentiel',
                'confort',
                'open',
                'initial',
                'confort_ttc',
                'pme',
                'pme_ece',
                'pme_eci',
                'exclusivité_multi',
                'lmnp_groupement',
                'sci_exclusivité',
                'sci_opportunité',
                'sci_opportunité_groupement',
                'sci_sérénité',
                'sci_sérénité_groupement'
                )) THEN 'Accounting'
        WHEN (LOWER(s.plan) IN (
                'opportunité_eci',
                'opportunité_ece',
                'exclusivité_ece',
                'exclusivité_eci',
                'sérénité_ece',
                'sérénité_eci',
                'liberté_ece',
                'liberté_eci',
                'ecommerce-engagement-320',
                'ecommerce-engagement-500',
                'exclusivité_multi_ece',
                'liberté_ei'
                ))THEN 'Accounting E-commerce'
    END AS plan_category,
    us.email AS email,
    s.trial_period_days,
    s.is_free_forever,
    s.period_ends_at,
    s.activated_at,
    s.created_at,
    s.updated_at,
    s.trial_started_at,
    s.free_month_count,
    s.company_id,
    s.suspended_at,
    s.is_annually_paid,
    s.is_unpaid_since,
    s.payment_method,
    s.custom_amount,
    s.end_of_trial_reason,
    s.end_of_trial_reason_comment,
    s.date_last_refresh
FROM {{ ref('stg_subscriptions') }} AS s
LEFT JOIN {{ ref('stg_companies') }} AS c 
    ON s.company_id = c.company_id
LEFT JOIN {{ ref('stg_users') }} AS us 
    ON us.preferred_company_id = c.company_id
