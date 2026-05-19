WITH unioned_all AS (
    SELECT
        a.date_report AS date,
        a.country,
        CAST(NULL AS STRING) AS first_subscription_plan,
        CAST(NULL AS STRING) AS last_subscription_plan,
        a.source_name,
        a.channel,
        CONCAT(revenue_window.value::STRING, 'M') AS revenue_window,
        CAST(NULL AS INTEGER) AS cohort_size,
        CAST(NULL AS INTEGER) AS paid_transaction_count,   
        CAST(NULL AS FLOAT) AS purchase_amount,
        CAST(NULL AS FLOAT) AS cost_amount,
        SUM(a.spend) AS spend,
        CAST(NULL AS INTEGER) AS new_signups
    FROM
        {{ ref('int_advertising_statistics') }} AS a,
        LATERAL FLATTEN(input => [1,2,3,4,5,6,7,8,9,10,11,12]) AS revenue_window
    WHERE
        a.date_report BETWEEN DATEADD('month', -1, CURRENT_DATE()) AND CURRENT_DATE()
        AND
        a.source_name IN (
            'Meta Ads',
            'Organic Social',
            'Google Ads'
        )
    GROUP BY
        date,
        country,
        source_name,
        channel,
        revenue_window

    UNION ALL

    SELECT
        s.signup_date AS date,
        s.country,
        s.first_subscription_plan,
        s.last_subscription_plan,
        s.first_touchpoint_source_name AS source_name,
        s.first_touchpoint_channel AS channel,
        CONCAT(revenue_window.value::STRING, 'M') AS revenue_window,
        CAST(NULL AS INTEGER) AS cohort_size,
        CAST(NULL AS INTEGER) AS paid_transaction_count,   
        CAST(NULL AS FLOAT) AS purchase_amount,
        CAST(NULL AS FLOAT) AS cost_amount,
        CAST(NULL AS FLOAT) AS spend,
        COUNT(DISTINCT s.workspace_id) AS new_signups
    FROM
        {{ ref('int_attributed_workspace_revenues') }} AS s,
        LATERAL FLATTEN(input => [1,2,3,4,5,6,7,8,9,10,11,12]) AS revenue_window
    WHERE
        s.signup_date BETWEEN DATEADD('month', -1, CURRENT_DATE()) AND CURRENT_DATE()
        AND
        scope IS NULL
        AND
        source_name IN (
            'Meta Ads',
            'Organic Social',
            'Google Ads'
        )
    GROUP BY
        date,
        country,
        first_subscription_plan,
        last_subscription_plan,
        source_name,
        channel,
        revenue_window

    UNION ALL
    
    SELECT
        w.signup_date AS date,
        w.country,
        w.first_subscription_plan,
        w.last_subscription_plan,
        w.first_touchpoint_source_name AS source_name,
        w.first_touchpoint_channel AS channel,
        CONCAT(revenue_window.value::STRING, 'M') AS revenue_window,
        COUNT(DISTINCT w.workspace_id) AS cohort_size,
        SUM(CASE WHEN w.value != 0 THEN 1 END) AS paid_transaction_count,   
        SUM(w.value) AS purchase_amount,
        CAST(NULL AS FLOAT) AS cost_amount,
        CAST(NULL AS FLOAT) AS spend,
        CAST(NULL AS INTEGER) AS new_signups
    FROM
        {{ ref('int_attributed_workspace_revenues') }} AS w,
        LATERAL FLATTEN(input => [1,2,3,4,5,6,7,8,9,10,11,12]) AS revenue_window
    WHERE
        w.scope = 'prestation'
        AND
        w.event_date BETWEEN w.signup_date AND DATEADD('month', revenue_window.value, w.signup_date)
        AND
        w.first_touchpoint_source_name IN (
            'Meta Ads',
            'Organic Social',
            'Google Ads'
        )        
    GROUP BY
        date,
        w.country,
        w.first_subscription_plan,
        w.last_subscription_plan,
        source_name,
        channel,
        revenue_window
    
    UNION ALL

    SELECT
        signup_date AS date,
        country,
        first_subscription_plan,
        last_subscription_plan,
        first_touchpoint_source_name AS source_name,
        first_touchpoint_channel AS channel,
        'lifetime' AS revenue_window,
        COUNT(DISTINCT workspace_id) AS cohort_size,
        SUM(CASE WHEN value != 0 THEN 1 END) AS paid_transaction_count,    
        SUM(value) AS purchase_amount,
        CAST(NULL AS FLOAT) AS cost_amount,
        CAST(NULL AS FLOAT) AS spend,
        CAST(NULL AS INTEGER) AS new_signups
    FROM
        {{ ref('int_attributed_workspace_revenues') }}
    WHERE
        scope = 'prestation'
        AND
        event_date >= signup_date
        AND
        first_touchpoint_source_name IN (
            'Meta Ads',
            'Organic Social',
            'Google Ads'
        )                
    GROUP BY
        date,
        country,
        first_subscription_plan,
        last_subscription_plan,
        source_name,
        channel,
        revenue_window

    UNION ALL
    
    SELECT
        w.signup_date AS date,
        w.country,
        w.first_subscription_plan,
        w.last_subscription_plan,
        w.first_touchpoint_source_name AS source_name,
        w.first_touchpoint_channel AS channel,
        CONCAT(revenue_window.value::STRING, 'M') AS revenue_window,        
        CAST(NULL AS INTEGER) AS cohort_size,
        CAST(NULL AS INTEGER) AS paid_transaction_count,        
        CAST(NULL AS FLOAT) AS purchase_amount,
        SUM(w.value) AS cost_amount,
        CAST(NULL AS FLOAT) AS spend,
        CAST(NULL AS INTEGER) AS new_signups
    FROM
        {{ ref('int_attributed_workspace_revenues') }} AS w,
        LATERAL FLATTEN(input => [1,2,3,4,5,6,7,8,9,10,11,12]) AS revenue_window
    WHERE
        w.scope = 'cost'
        AND
        w.event_date BETWEEN w.signup_date AND DATEADD('month', revenue_window.value, w.signup_date)
        AND
        w.first_touchpoint_source_name IN (
            'Meta Ads',
            'Organic Social',
            'Google Ads'
        )                
    GROUP BY
        date,
        country,
        first_subscription_plan,
        last_subscription_plan,
        source_name,
        channel,
        revenue_window

    UNION ALL
    
    SELECT
        signup_date AS date,
        country,
        first_subscription_plan,
        last_subscription_plan,
        first_touchpoint_source_name AS source_name,
        first_touchpoint_channel AS channel,
        'lifetime' AS revenue_window,
        CAST(NULL AS INTEGER) AS cohort_size,
        CAST(NULL AS INTEGER) AS paid_transaction_count,        
        CAST(NULL AS FLOAT) AS purchase_amount,
        SUM(value) AS cost_amount,
        CAST(NULL AS FLOAT) AS spend,
        CAST(NULL AS INTEGER) AS new_signups
    FROM
        {{ ref('int_attributed_workspace_revenues') }}
    WHERE
        scope = 'cost'
        AND
        event_date >= signup_date
        AND
        first_touchpoint_source_name IN (
            'Meta Ads',
            'Organic Social',
            'Google Ads'
        )                
    GROUP BY
        date,
        country,
        first_subscription_plan,
        last_subscription_plan,
        source_name,
        channel,
        revenue_window
),

real_revenues_spend AS (
    SELECT
        date,
        country,
        first_subscription_plan,
        last_subscription_plan,
        source_name,
        channel,
        revenue_window,
        NULLIF(SUM(spend),0) AS spend,
        NULLIF(SUM(new_signups),0) AS new_signups,
        NULLIF(SUM(cohort_size),0) AS cohort_size,
        NULLIF(SUM(paid_transaction_count),0) AS paid_transaction_count,        
        NULLIF(SUM(purchase_amount),0) AS gross_revenues,
        NULLIF(SUM(purchase_amount),0)-NULLIF(SUM(cost_amount),0) AS net_revenues
    FROM
        unioned_all
    GROUP BY
        date,
        country,
        first_subscription_plan,
        last_subscription_plan,
        source_name,
        channel,
        revenue_window
    HAVING
        NULLIF(SUM(spend),0) IS NOT NULL
        OR NULLIF(SUM(new_signups),0) IS NOT NULL
        OR NULLIF(SUM(cohort_size), 0) IS NOT NULL
        OR NULLIF(SUM(paid_transaction_count),0) IS NOT NULL
        OR NULLIF(SUM(purchase_amount),0) IS NOT NULL
        OR NULLIF(SUM(cost_amount),0) IS NOT NULL
),

projected_revenues AS (
    SELECT
        projected_months.month AS date,
        rr.source_name,
        rr.channel,
        rr.revenue_window,
        SUM(rr.spend) AS spend,
        SUM(rr.new_signups) AS new_signups,
        SUM(rr.cohort_size) AS cohort_size,
        SUM(rr.paid_transaction_count) AS paid_transaction_count,
        SUM(rr.gross_revenues) AS gross_revenues_after_signup,
        SUM(rr.net_revenues) AS net_revenues_after_signup,
    FROM
        (
            SELECT 
                DATEADD(MONTH, SEQ4(), DATEADD('month', 1, CURRENT_DATE())) AS month
            FROM 
                TABLE(GENERATOR(ROWCOUNT => 12))
        ) projected_months
            LEFT JOIN
                real_revenues_spend rr
            ON
                (rr.revenue_window = '1M' AND rr.date BETWEEN DATEADD('month', -2, CURRENT_DATE()) AND DATEADD('month', -1, CURRENT_DATE()))
                OR (rr.revenue_window = '2M' AND rr.date BETWEEN DATEADD('month', -4, CURRENT_DATE()) AND DATEADD('month', -2, CURRENT_DATE()))
                OR (rr.revenue_window = '3M' AND rr.date BETWEEN DATEADD('month', -6, CURRENT_DATE()) AND DATEADD('month', -3, CURRENT_DATE()))
                OR (rr.revenue_window = '4M' AND rr.date BETWEEN DATEADD('month', -8, CURRENT_DATE()) AND DATEADD('month', -4, CURRENT_DATE()))
                OR (rr.revenue_window = '5M' AND rr.date BETWEEN DATEADD('month', -10, CURRENT_DATE()) AND DATEADD('month', -5, CURRENT_DATE())) 
                OR (rr.revenue_window = '6M' AND rr.date BETWEEN DATEADD('month', -12, CURRENT_DATE()) AND DATEADD('month', -6, CURRENT_DATE())) 
                OR (rr.revenue_window = '7M' AND rr.date BETWEEN DATEADD('month', -14, CURRENT_DATE()) AND DATEADD('month', -7, CURRENT_DATE()))
                OR (rr.revenue_window = '8M' AND rr.date BETWEEN DATEADD('month', -16, CURRENT_DATE()) AND DATEADD('month', -8, CURRENT_DATE())) 
                OR (rr.revenue_window = '9M' AND rr.date BETWEEN DATEADD('month', -18, CURRENT_DATE()) AND DATEADD('month', -9, CURRENT_DATE()))
                OR (rr.revenue_window = '10M' AND rr.date BETWEEN DATEADD('month', -20, CURRENT_DATE()) AND DATEADD('month', -10, CURRENT_DATE())) 
                OR (rr.revenue_window = '11M' AND rr.date BETWEEN DATEADD('month', -22, CURRENT_DATE()) AND DATEADD('month', -11, CURRENT_DATE())) 
                OR (rr.revenue_window = '12M' AND rr.date BETWEEN DATEADD('month', -24, CURRENT_DATE()) AND DATEADD('month', -12, CURRENT_DATE()))
                OR
                (rr.spend IS NOT NULL)
                OR
                (rr.new_signups IS NOT NULL)
        WHERE
            (projected_months.month = DATEADD('month', 1, CURRENT_DATE()) AND rr.revenue_window = '1M')
            OR (projected_months.month = DATEADD('month', 2, CURRENT_DATE()) AND rr.revenue_window = '2M')
            OR (projected_months.month = DATEADD('month', 3, CURRENT_DATE()) AND rr.revenue_window = '3M') 
            OR (projected_months.month = DATEADD('month', 4, CURRENT_DATE()) AND rr.revenue_window = '4M')  
            OR (projected_months.month = DATEADD('month', 5, CURRENT_DATE()) AND rr.revenue_window = '5M')  
            OR (projected_months.month = DATEADD('month', 6, CURRENT_DATE()) AND rr.revenue_window = '6M')  
            OR (projected_months.month = DATEADD('month', 7, CURRENT_DATE()) AND rr.revenue_window = '7M')  
            OR (projected_months.month = DATEADD('month', 8, CURRENT_DATE()) AND rr.revenue_window = '8M')  
            OR (projected_months.month = DATEADD('month', 9, CURRENT_DATE()) AND rr.revenue_window = '9M')  
            OR (projected_months.month = DATEADD('month', 10, CURRENT_DATE()) AND rr.revenue_window = '10M')  
            OR (projected_months.month = DATEADD('month', 11, CURRENT_DATE()) AND rr.revenue_window = '11M')  
            OR (projected_months.month = DATEADD('month', 12, CURRENT_DATE()) AND rr.revenue_window = '12M')          
        GROUP BY
            projected_months.month,
            rr.country,
            rr.first_subscription_plan,
            rr.last_subscription_plan,
            rr.source_name,
            rr.channel,
            rr.revenue_window
),

forecasts AS (
    SELECT
        date,
        source_name,
        channel,
        revenue_window,
        NULLIF(SUM(spend), 0) AS spend,
        NULLIF(SUM(new_signups), 0) AS new_signups,
        NULLIF(ROUND(SUM(new_signups * DIV0(gross_revenues_after_signup, cohort_size)), 5), 0) AS gross_revenues_after_signup,        
        NULLIF(ROUND(SUM(new_signups * DIV0(net_revenues_after_signup, cohort_size)), 5), 0) AS net_revenues_after_signup,
        NULLIF(ROUND(AVG(DIV0(gross_revenues_after_signup, cohort_size)), 5), 0) AS gross_ltv_after_signup,
        NULLIF(ROUND(AVG(DIV0(net_revenues_after_signup, cohort_size)), 5), 0) AS net_ltv_after_signup
    FROM
        projected_revenues
    GROUP BY
        date,
        source_name,
        channel,
        revenue_window
)

SELECT
    date,
    source_name,
    channel,
    revenue_window,
    SUM(spend) AS spend,
    SUM(new_signups) AS new_signups,
    SUM(gross_revenues_after_signup) AS gross_revenues_after_signup,
    SUM(net_revenues_after_signup) AS net_revenues_after_signup,
    AVG(gross_ltv_after_signup) AS gross_ltv_after_signup,
    AVG(net_ltv_after_signup) AS net_ltv_after_signup
FROM
    forecasts
GROUP BY
    date,
    source_name,
    channel,
    revenue_window
