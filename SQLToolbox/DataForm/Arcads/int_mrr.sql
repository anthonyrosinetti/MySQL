WITH date_spine AS (
    -- SEQ4() starts at 0 → first month_start = 2020-01-01
    SELECT
        DATEADD(month, SEQ4(), '2020-01-01'::DATE)::DATE AS month_start
    FROM
        TABLE(GENERATOR(ROWCOUNT => 20 * 12))
),

-- One FX rate per calendar month: last available close_rate for that month
monthly_fx_rates AS (
    SELECT
        DATE_TRUNC('month', date)::DATE     AS month_start,
        close_rate
    FROM
        {{ ref('fct_currency_rates_eur_usd') }}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', date) ORDER BY date DESC) = 1
),

-- ─────────────────────────────────────────
-- PART 1 — PAST MONTHS (invoice-based)
-- ─────────────────────────────────────────

paid_invoices AS (
    SELECT
        id              AS invoice_id,
        customer_id,
        subscription_id,
        currency,
        -- Ratio < 1 when a coupon was applied; 1.0 when no discount.
        -- Using tax-exclusive figures so the factor is purely the discount effect.
        COALESCE(total_excluding_tax, total)
            / NULLIF(COALESCE(subtotal_excluding_tax, subtotal), 0) AS discount_factor
    FROM
        {{ source('stripe', 'INVOICE') }}
    WHERE
        status              = 'paid'
        AND subscription_id IS NOT NULL
        AND COALESCE(is_deleted, FALSE) IS DISTINCT FROM TRUE
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_synced DESC) = 1
),

invoice_line_items AS (
    SELECT
        ili.unique_id,
        ili.invoice_id,
        ili.subscription_id,
        ili.subscription_item_id,
        ili.plan_id,
        ili.metadata['workspaceId']::STRING AS workspace_id,
        CAST(ili.period_start AS DATE)  AS period_start,
        CAST(ili.period_end   AS DATE)  AS period_end,
        ili.amount,
        -- Fall back to amount when amount_excluding_tax is not populated (older records)
        COALESCE(ili.amount_excluding_tax, ili.amount) AS amount_excl_tax,
        pi.currency,
        pi.customer_id,
        pi.discount_factor
    FROM
        {{ source('stripe', 'INVOICE_LINE_ITEM') }} ili
    JOIN
        paid_invoices pi ON ili.invoice_id = pi.invoice_id
    WHERE
        -- Recurring subscription charges only; excludes one-time invoice items
        ili.type    = 'subscription'
        -- Exclude mid-cycle proration adjustments from plan changes
        AND COALESCE(ili.proration, FALSE) = FALSE
        -- Exclude $0 lines (free trials, fully-discounted items)
        AND ili.amount > 0
        AND ili.period_start IS NOT NULL
        AND ili.period_end   IS NOT NULL
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY ili.unique_id ORDER BY ili._fivetran_synced DESC) = 1
),

-- Spread each line item evenly across the calendar months it covers:
--   monthly plan  → 1 month  → full amount to that month
--   annual plan   → 12 months → amount / 12 to each month
-- The date-spine join handles all interval lengths automatically.
invoice_monthly_mrr AS (
    SELECT
        month_start, subscription_id, subscription_item_id, plan_id, customer_id, workspace_id, currency,
        mrr,
        mrr_excl_tax,
        mrr          * discount_factor  AS mrr_after_discount,
        mrr_excl_tax * discount_factor  AS mrr_after_discount_excl_tax
    FROM (
        SELECT
            d.month_start,
            ili.subscription_id,
            ili.subscription_item_id,
            ili.plan_id,
            ili.customer_id,
            ili.workspace_id,
            ili.currency,
            ili.amount / NULLIF(
                DATEDIFF('month',
                    DATE_TRUNC('month', ili.period_start)::DATE,
                    DATE_TRUNC('month', ili.period_end)::DATE
                ), 0
            )                           AS mrr,
            ili.amount_excl_tax / NULLIF(
                DATEDIFF('month',
                    DATE_TRUNC('month', ili.period_start)::DATE,
                    DATE_TRUNC('month', ili.period_end)::DATE
                ), 0
            )                           AS mrr_excl_tax,
            ili.discount_factor
        FROM
            invoice_line_items ili
        JOIN
            date_spine d
                ON  d.month_start >= DATE_TRUNC('month', ili.period_start)::DATE
                AND d.month_start <  DATE_TRUNC('month', ili.period_end)::DATE
        WHERE
            -- Past months only; current month is handled by the subscription block below
            d.month_start < DATE_TRUNC('month', CURRENT_DATE)::DATE
    )
),

-- ─────────────────────────────────────────────────────────────
-- PART 2 — CURRENT MONTH (subscription-based)
-- ─────────────────────────────────────────────────────────────

price_plan AS (
    SELECT *
    FROM {{ source('stripe', 'PLAN') }}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_synced DESC, created DESC) = 1
),

subscription_latest AS (
    SELECT
        id                  AS subscription_id,
        customer_id,
        metadata['workspaceId']::STRING AS workspace_id,
        status              AS subscription_status,
        current_period_end
    FROM
        {{ source('stripe', 'SUBSCRIPTION_HISTORY') }}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_synced DESC) = 1
),

-- One item per subscription: most recently created = current plan
subscription_item_current AS (
    SELECT *
    FROM {{ source('stripe', 'SUBSCRIPTION_ITEM') }}
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY created DESC, _fivetran_synced DESC) = 1
),

coupon_deduped AS (
    SELECT
        id              AS coupon_id,
        percent_off,
        amount_off
    FROM {{ source('stripe', 'COUPON') }}
    WHERE COALESCE(is_deleted, FALSE) IS DISTINCT FROM TRUE
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _fivetran_synced DESC) = 1
),

-- Active subscription-level discount today; subscription-level takes precedence over customer-level
subscription_discount_current AS (
    SELECT
        sd.subscription_id,
        c.percent_off,
        c.amount_off
    FROM {{ source('stripe', 'SUBSCRIPTION_DISCOUNT') }} sd
    JOIN coupon_deduped c ON c.coupon_id = sd.coupon_id
    WHERE CAST(sd."START" AS DATE) <= CURRENT_DATE
      AND (sd."END" IS NULL OR CAST(sd."END" AS DATE) > CURRENT_DATE)
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY sd.subscription_id ORDER BY sd."START" DESC) = 1
),

-- Active customer-level discount today (subscription_id IS NULL = applies to all subscriptions)
customer_discount_current AS (
    SELECT
        cd.customer_id,
        c.percent_off,
        c.amount_off
    FROM {{ source('stripe', 'CUSTOMER_DISCOUNT') }} cd
    JOIN coupon_deduped c ON c.coupon_id = cd.coupon_id
    WHERE cd.subscription_id IS NULL
      AND CAST(cd."START" AS DATE) <= CURRENT_DATE
      AND (cd."END" IS NULL OR CAST(cd."END" AS DATE) > CURRENT_DATE)
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY cd.customer_id ORDER BY cd."START" DESC) = 1
),

current_month_mrr AS (
    SELECT
        month_start, subscription_id, subscription_item_id, plan_id, customer_id, workspace_id, currency,
        mrr,
        mrr_excl_tax,
        -- Effective discount: subscription-level overrides customer-level
        CASE
            WHEN eff_percent_off IS NOT NULL
                THEN mrr * (1.0 - eff_percent_off / 100.0)
            WHEN eff_amount_off IS NOT NULL
                THEN GREATEST(mrr - eff_amount_off, 0)
            ELSE mrr
        END                                         AS mrr_after_discount,
        CASE
            WHEN eff_percent_off IS NOT NULL
                THEN mrr_excl_tax * (1.0 - eff_percent_off / 100.0)
            WHEN eff_amount_off IS NOT NULL
                THEN GREATEST(mrr_excl_tax - eff_amount_off, 0)
            ELSE mrr_excl_tax
        END                                         AS mrr_after_discount_excl_tax
    FROM (
        SELECT
            DATE_TRUNC('month', CURRENT_DATE)::DATE     AS month_start,
            sl.subscription_id,
            si.id                                       AS subscription_item_id,
            si.plan_id,
            sl.customer_id,
            sl.workspace_id,
            pp.currency,
            CASE
                WHEN LOWER(pp.interval) = 'week'  THEN (pp.amount * COALESCE(si.quantity, 1) * 52.0 / 12.0) / NULLIF(pp.interval_count, 0)
                WHEN LOWER(pp.interval) = 'month' THEN  pp.amount * COALESCE(si.quantity, 1)               / NULLIF(pp.interval_count, 0)
                WHEN LOWER(pp.interval) = 'year'  THEN (pp.amount * COALESCE(si.quantity, 1) / 12.0)       / NULLIF(pp.interval_count, 0)
            END                                         AS mrr,
            -- Stripe plan amounts are always pre-tax, so mrr_excl_tax = mrr for current month
            CASE
                WHEN LOWER(pp.interval) = 'week'  THEN (pp.amount * COALESCE(si.quantity, 1) * 52.0 / 12.0) / NULLIF(pp.interval_count, 0)
                WHEN LOWER(pp.interval) = 'month' THEN  pp.amount * COALESCE(si.quantity, 1)               / NULLIF(pp.interval_count, 0)
                WHEN LOWER(pp.interval) = 'year'  THEN (pp.amount * COALESCE(si.quantity, 1) / 12.0)       / NULLIF(pp.interval_count, 0)
            END                                         AS mrr_excl_tax,
            COALESCE(sdc.percent_off, cdc.percent_off)  AS eff_percent_off,
            COALESCE(sdc.amount_off,  cdc.amount_off)   AS eff_amount_off
        FROM
            subscription_latest sl
        JOIN
            subscription_item_current si ON sl.subscription_id = si.subscription_id
        LEFT JOIN
            price_plan pp ON si.plan_id = pp.id
        LEFT JOIN
            subscription_discount_current sdc ON sdc.subscription_id = sl.subscription_id
        LEFT JOIN
            customer_discount_current cdc ON cdc.customer_id = sl.customer_id
        WHERE
            sl.subscription_status IN ('active', 'trialing', 'past_due')
            AND sl.current_period_end >= DATE_TRUNC('month', CURRENT_DATE)
    )
),

-- ─────────────────────────────────────────
-- COMBINE (subscription level)
-- ─────────────────────────────────────────

all_mrr_raw AS (
    SELECT month_start, subscription_id, subscription_item_id, plan_id, customer_id, workspace_id, currency, mrr, mrr_excl_tax, mrr_after_discount, mrr_after_discount_excl_tax
    FROM invoice_monthly_mrr

    UNION ALL

    SELECT month_start, subscription_id, subscription_item_id, plan_id, customer_id, workspace_id, currency, mrr, mrr_excl_tax, mrr_after_discount, mrr_after_discount_excl_tax
    FROM current_month_mrr
),

-- Convert all amounts to EUR.
-- USD → divide by close_rate. EUR or NULL currency → already in EUR, no change.
all_mrr AS (
    SELECT
        r.month_start,
        r.subscription_id,
        r.subscription_item_id,
        r.plan_id,
        r.customer_id,
        r.workspace_id,
        r.currency,
        CASE
            WHEN LOWER(r.currency) = 'usd' THEN r.mrr                       / NULLIF(fx.close_rate, 0)
            ELSE r.mrr
        END                             AS mrr,
        CASE
            WHEN LOWER(r.currency) = 'usd' THEN r.mrr_excl_tax              / NULLIF(fx.close_rate, 0)
            ELSE r.mrr_excl_tax
        END                             AS mrr_excl_tax,
        CASE
            WHEN LOWER(r.currency) = 'usd' THEN r.mrr_after_discount        / NULLIF(fx.close_rate, 0)
            ELSE r.mrr_after_discount
        END                             AS mrr_after_discount,
        CASE
            WHEN LOWER(r.currency) = 'usd' THEN r.mrr_after_discount_excl_tax / NULLIF(fx.close_rate, 0)
            ELSE r.mrr_after_discount_excl_tax
        END                             AS mrr_after_discount_excl_tax
    FROM
        all_mrr_raw r
    LEFT JOIN
        monthly_fx_rates fx ON fx.month_start = r.month_start
),

-- ─────────────────────────────────────────────────────────────
-- PART 3 — MRR MOVEMENT TYPE (computed at customer level)
-- ─────────────────────────────────────────────────────────────

-- Aggregate to customer level in native currency — used for movement classification only.
-- Keeping native currency avoids false expansion/contraction from FX rate fluctuations.
customer_monthly_mrr AS (
    SELECT
        month_start,
        customer_id,
        workspace_id,
        currency,
        SUM(mrr)    AS mrr
    FROM all_mrr_raw
    GROUP BY 1, 2, 3, 4
),

-- Dense month spine per customer so $0 gaps appear explicitly.
-- Range: customer's first-ever MRR month → current month.
customer_month_spine AS (
    SELECT
        d.month_start,
        c.customer_id,
        c.workspace_id,
        c.currency,
        COALESCE(m.mrr, 0)  AS mrr
    FROM (
        SELECT customer_id, workspace_id, currency, MIN(month_start) AS first_month
        FROM customer_monthly_mrr
        GROUP BY 1, 2, 3
    ) c
    JOIN date_spine d
        ON  d.month_start >= c.first_month
        AND d.month_start <= DATE_TRUNC('month', CURRENT_DATE)::DATE
    LEFT JOIN customer_monthly_mrr m
        ON  m.customer_id  = c.customer_id
        AND m.currency     = c.currency
        AND m.month_start  = d.month_start
),

-- Lag + prior-MRR flag, in native currency (no FX noise)
customer_mrr_classified AS (
    SELECT
        month_start,
        customer_id,
        workspace_id,
        currency,
        mrr                                                         AS customer_mrr,
        COALESCE(
            LAG(mrr) OVER (PARTITION BY customer_id, currency ORDER BY month_start),
            0
        )                                                           AS customer_prev_mrr,
        COALESCE(
            MAX(IFF(mrr > 0, 1, 0)) OVER (
                PARTITION BY customer_id, currency
                ORDER BY month_start
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0
        )                                                           AS had_prior_mrr
    FROM customer_month_spine
),

-- Classify each customer-month into a movement type
customer_mrr_movements AS (
    SELECT
        month_start,
        customer_id,
        workspace_id,
        currency,
        customer_mrr        / 100.0     AS customer_mrr,
        customer_prev_mrr   / 100.0     AS customer_prev_mrr,
        CASE
            WHEN customer_mrr > 0 AND customer_prev_mrr = 0 AND had_prior_mrr = 0   THEN 'new'
            WHEN customer_mrr > 0 AND customer_prev_mrr = 0 AND had_prior_mrr = 1   THEN 'reactivation'
            WHEN customer_mrr > customer_prev_mrr AND customer_prev_mrr > 0          THEN 'expansion'
            WHEN customer_mrr < customer_prev_mrr AND customer_mrr > 0              THEN 'contraction'
            WHEN customer_mrr = customer_prev_mrr AND customer_mrr > 0              THEN 'retained'
            WHEN customer_mrr = 0 AND customer_prev_mrr > 0                         THEN 'churned'
        END                                                         AS mrr_type,
        -- Delta: full amount for new/reactivation/retained/churned; incremental for expansion/contraction
        CASE
            WHEN customer_mrr > 0 AND customer_prev_mrr = 0                         THEN  customer_mrr / 100.0
            WHEN customer_mrr > customer_prev_mrr AND customer_prev_mrr > 0         THEN (customer_mrr - customer_prev_mrr) / 100.0
            WHEN customer_mrr < customer_prev_mrr AND customer_mrr > 0             THEN (customer_mrr - customer_prev_mrr) / 100.0
            WHEN customer_mrr = customer_prev_mrr AND customer_mrr > 0             THEN  customer_mrr / 100.0
            WHEN customer_mrr = 0 AND customer_prev_mrr > 0                        THEN -customer_prev_mrr / 100.0
        END                                                         AS customer_mrr_delta
    FROM customer_mrr_classified
    WHERE NOT (customer_mrr = 0 AND customer_prev_mrr = 0)
),

hubspot_ids AS (
    SELECT DISTINCT
        property_workspace_id,
        id
    FROM
        {{ source('hubspot', 'WORKSPACES') }}
),

workspaces_stages AS (
    SELECT DISTINCT
        workspace_id,
        country,
        first_subscription_plan,
        last_subscription_plan,
        first_touchpoint_at,
        first_touchpoint_partner_referral_id,
        first_touchpoint_source,
        first_touchpoint_medium,
        first_touchpoint_campaign
    FROM
       int_workspace_stages_attribution
)

-- ─────────────────────────────────────────────────────────────
-- FINAL: subscription-level detail + customer-level movement type
-- ─────────────────────────────────────────────────────────────
SELECT
    a.month_start AS date_report,
    a.subscription_id,
    a.subscription_item_id,
    a.plan_id,
    a.customer_id,
    w.id AS workspace_id,
    ws.country,
    ws.first_subscription_plan,
    ws.last_subscription_plan,
    /*
    CASE
        WHEN ws.first_touchpoint_at IS NULL THEN 'No touchpoint'
        WHEN ws.first_touchpoint_partner_referral_id IS NOT NULL THEN 'Parrainage'
        ELSE IFNULL(FIRST_VALUE(s.source_name) OVER (PARTITION BY ws.first_touchpoint_source, ws.first_touchpoint_medium
            ORDER BY s.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),'Unmapped')
        -- ELSE first_touchpoint_source
    END AS first_touchpoint_source_name,
    CASE
        WHEN ws.first_touchpoint_at IS NULL THEN 'No touchpoint'
        WHEN ws.first_touchpoint_partner_referral_id IS NOT NULL THEN 'Parrainage'
        ELSE IFNULL(FIRST_VALUE(s.channel) OVER (PARTITION BY ws.first_touchpoint_source, ws.first_touchpoint_medium
            ORDER BY s.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),'Unmapped')
        -- ELSE first_touchpoint_medium
    END AS first_touchpoint_channel,    
    */
    a.currency,
    a.mrr                            / 100.0  AS mrr_eur,
    a.mrr_excl_tax                   / 100.0  AS mrr_excl_tax_eur,
    a.mrr_after_discount             / 100.0  AS mrr_after_discount_eur,
    a.mrr_after_discount_excl_tax    / 100.0  AS mrr_after_discount_excl_tax_eur,
    m.mrr_type,
    m.customer_mrr              AS customer_mrr_native,
    m.customer_prev_mrr         AS customer_prev_mrr_native,
    m.customer_mrr_delta        AS customer_mrr_delta_native
FROM
    all_mrr a
LEFT JOIN
    customer_mrr_movements m
        ON  a.customer_id  = m.customer_id
        AND a.workspace_id  = m.workspace_id
        AND a.currency     = m.currency
        AND a.month_start  = m.month_start
LEFT JOIN
    hubspot_ids AS w
        ON  w.property_workspace_id  = a.workspace_id
LEFT JOIN
    workspaces_stages ws
        ON  ws.workspace_id = w.id
        /*
LEFT JOIN
    ANALYTICS.MARKETING.dim_mapping_source_medium AS s
ON
    (s.utm_source IS NULL OR LOWER(ws.first_touchpoint_source) LIKE LOWER(s.utm_source))
    AND
    (s.utm_medium IS NULL OR LOWER(ws.first_touchpoint_medium) LIKE LOWER(s.utm_medium))    
LEFT JOIN
    ANALYTICS.MARKETING.dim_mapping_campaign AS c
ON
    (c.utm_campaign IS NULL OR LOWER(ws.first_touchpoint_campaign) LIKE LOWER(c.utm_campaign))
    AND
    (c.utm_source IS NULL OR LOWER(ws.first_touchpoint_source) LIKE LOWER(c.utm_source))
    AND
    (c.utm_medium IS NULL OR LOWER(ws.first_touchpoint_medium) LIKE LOWER(c.utm_medium))         
    */
