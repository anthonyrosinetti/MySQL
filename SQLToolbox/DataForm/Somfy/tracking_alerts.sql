-- Config block
config {
    type: "incremental",
    schema: "tracking_monitoring",
    protected: true,
    partitionBy: "TIMESTAMP_TRUNC(_ingestion_timestamp, DAY)"
}

-- SQL
WITH events_scope AS (
    SELECT
        *
    FROM
        ${ref("piano_analytics_raw_events")}
    WHERE
        date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) AND CURRENT_DATE() -- zero_event
        OR
        date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 4 + 1 WEEK) AND CURRENT_DATE() -- relative_variation -35%
),

last_alert AS (
    SELECT DISTINCT
        alert_type,
        event_name,
        website,
        LAST_VALUE(_ingestion_timestamp) OVER w AS last_alert_timestamp,
        -- LAST_VALUE(previous_alert_timestamp) OVER w AS previous_alert_timestamp,
        LAST_VALUE(first_alert_timestamp) OVER w AS first_alert_timestamp,
        -- LAST_VALUE(is_new_alert) OVER w AS is_new_alert
    FROM
        ${self()}
    WINDOW
        w AS (
            PARTITION BY alert_type, event_name, website
            ORDER BY _ingestion_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),

all_events_zero_event AS (
    SELECT DISTINCT
        pa.event_name,
        pa.site_id,
        cf.website
    FROM
        ${ref("daily_website_events")} AS pa
    LEFT JOIN
        ${ref("piano_websites")} AS cf
    USING
        (site_id)     
    WHERE
        pa.event_name NOT LIKE "%exclusion%"
        AND pa.event_name NOT IN (
            'click.campaign.offsite'
        )
        AND pa.date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND CURRENT_DATE()
        AND IFNULL(cf.tracking_alert_status, FALSE) = TRUE
),

all_events_relative_variation AS (
    SELECT DISTINCT
        pa.event_name,
        pa.site_id,
        cf.website
    FROM
        ${ref("daily_website_events")} AS pa
    LEFT JOIN
        ${ref("piano_websites")} AS cf
    USING
        (site_id)     
    WHERE
        pa.event_name IN (
            'page.display',
            'click.action',
            'click.download',
            'product.display',
            'product.page_display',
            'product.add_to_cart',
            'product.cart',
            'product.delivery',
            'product.payment',
            'product.purchased',
            'transaction.confirmation',
            'cart.display',
            'cart.delivery',
            'cart.payment',
            'internal_search_result.display'
        ) 
        AND pa.date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND CURRENT_DATE()
        AND IFNULL(cf.tracking_alert_status, FALSE) = TRUE
),

relative_variation_pre_synchro_1 AS (
    SELECT
        date,
        CASE
            WHEN a.event_name = 'click.action' THEN CONCAT(a.event_name,' - ',e_monitored.click)
            ELSE a.event_name
        END AS event_name,     
        a.website,      
        SUM(e_monitored.m_events) AS monitored_event_count,
        CAST(NULL AS INT64) AS minus_1_week_event_count,
        CAST(NULL AS INT64) AS minus_2_week_event_count,
        CAST(NULL AS INT64) AS minus_3_week_event_count,
        CAST(NULL AS INT64) AS minus_4_week_event_count
    FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) date
        LEFT JOIN
            events_scope e_monitored
        ON
            e_monitored.date = date
        INNER JOIN
            all_events_relative_variation a
        ON
            a.event_name = e_monitored.event_name
            AND a.site_id = e_monitored.site_id
    WHERE
        (
            a.event_name != 'click.action'
        )
        OR
        (
            a.event_name = 'click.action'
            AND
            e_monitored.click IN (
                'lead_confirmation',
                'searchbutton_click',
                'download'
            )   
        )         
    GROUP BY
        date,
        event_name,
        click,
        website
    HAVING
        NULLIF(SUM(e_monitored.m_events),0) IS NOT NULL

    UNION ALL

    SELECT
        date,
        CASE
            WHEN a.event_name = 'click.action' THEN CONCAT(a.event_name,' - ',e_minus_1_week.click)
            ELSE a.event_name
        END AS event_name,  
        a.website,      
        CAST(NULL AS INT64) AS monitored_event_count,
        SUM(e_minus_1_week.m_events) AS minus_1_week_event_count,
        CAST(NULL AS INT64) AS minus_2_week_event_count,
        CAST(NULL AS INT64) AS minus_3_week_event_count,
        CAST(NULL AS INT64) AS minus_4_week_event_count
    FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) date
        LEFT JOIN
            events_scope e_minus_1_week
        ON
            date = DATE_ADD(e_minus_1_week.date, INTERVAL 1 WEEK)
        INNER JOIN
            all_events_relative_variation a
        ON
            a.event_name = e_minus_1_week.event_name
            AND a.site_id = e_minus_1_week.site_id
    WHERE
        (
            a.event_name != 'click.action'
        )
        OR
        (
            a.event_name = 'click.action'
            AND
            e_minus_1_week.click IN (
                'lead_confirmation',
                'searchbutton_click',
                'download'
            )   
        )                    
    GROUP BY
        date,
        event_name,
        click,
        website
    HAVING
        NULLIF(SUM(e_minus_1_week.m_events),0) IS NOT NULL

    UNION ALL

    SELECT
        date,
        CASE
            WHEN a.event_name = 'click.action' THEN CONCAT(a.event_name,' - ',e_minus_2_week.click)
            ELSE a.event_name
        END AS event_name,  
        a.website,      
        CAST(NULL AS INT64) AS monitored_event_count,
        CAST(NULL AS INT64) AS minus_1_week_event_count,
        SUM(e_minus_2_week.m_events) AS minus_2_week_event_count,
        CAST(NULL AS INT64) AS minus_3_week_event_count,
        CAST(NULL AS INT64) AS minus_4_week_event_count
    FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) date
        LEFT JOIN
            events_scope e_minus_2_week
        ON
            date = DATE_ADD(e_minus_2_week.date, INTERVAL 2 WEEK)
        INNER JOIN
            all_events_relative_variation a
        ON
            a.event_name = e_minus_2_week.event_name
            AND a.site_id = e_minus_2_week.site_id
    WHERE
        (
            a.event_name != 'click.action'
        )
        OR
        (
            a.event_name = 'click.action'
            AND
            e_minus_2_week.click IN (
                'lead_confirmation',
                'searchbutton_click',
                'download'
            )   
        )                  
    GROUP BY
        date,
        event_name,
        click,
        website
    HAVING
        NULLIF(SUM(e_minus_2_week.m_events),0) IS NOT NULL

    UNION ALL

    SELECT
        date,
        CASE
            WHEN a.event_name = 'click.action' THEN CONCAT(a.event_name,' - ',e_minus_3_week.click)
            ELSE a.event_name
        END AS event_name, 
        a.website,      
        CAST(NULL AS INT64) AS monitored_event_count,
        CAST(NULL AS INT64) AS minus_1_week_event_count,
        CAST(NULL AS INT64) AS minus_2_week_event_count,
        SUM(e_minus_3_week.m_events) AS minus_3_week_event_count,
        CAST(NULL AS INT64) AS minus_4_week_event_count
    FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) date
        LEFT JOIN
            events_scope e_minus_3_week
        ON
            date = DATE_ADD(e_minus_3_week.date, INTERVAL 3 WEEK)
        INNER JOIN
            all_events_relative_variation a
        ON
            a.event_name = e_minus_3_week.event_name
            AND a.site_id = e_minus_3_week.site_id
    WHERE
        (
            a.event_name != 'click.action'
        )
        OR
        (
            a.event_name = 'click.action'
            AND
            e_minus_3_week.click IN (
                'lead_confirmation',
                'searchbutton_click',
                'download'
            )   
        )             
    GROUP BY
        date,
        event_name,
        click,
        website
    HAVING
        NULLIF(SUM(e_minus_3_week.m_events),0) IS NOT NULL      

    UNION ALL

    SELECT
        date,
        CASE
            WHEN a.event_name = 'click.action' THEN CONCAT(a.event_name,' - ',e_minus_4_week.click)
            ELSE a.event_name
        END AS event_name, 
        a.website,      
        CAST(NULL AS INT64) AS monitored_event_count,
        CAST(NULL AS INT64) AS minus_1_week_event_count,
        CAST(NULL AS INT64) AS minus_2_week_event_count,
        CAST(NULL AS INT64) AS minus_3_week_event_count,
        SUM(e_minus_4_week.m_events) AS minus_4_week_event_count
    FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY),DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) date
        LEFT JOIN
            events_scope e_minus_4_week
        ON
            date = DATE_ADD(e_minus_4_week.date, INTERVAL 4 WEEK)
        INNER JOIN
            all_events_relative_variation a
        ON
            a.event_name = e_minus_4_week.event_name
            AND a.site_id = e_minus_4_week.site_id
    WHERE
        (
            a.event_name != 'click.action'
        )
        OR
        (
            a.event_name = 'click.action'
            AND
            e_minus_4_week.click IN (
                'lead_confirmation',
                'searchbutton_click',
                'download'
            )   
        )                
    GROUP BY
        date,
        event_name,
        click,
        website
    HAVING
        NULLIF(SUM(e_minus_4_week.m_events),0) IS NOT NULL  
),

-- 'l4w' for 'last 4 weeks'
relative_variation_pre_synchro_2 AS (
    SELECT
        date,
        event_name,
        website,
        SUM(monitored_event_count) AS monitored_event_count,
        SUM(minus_1_week_event_count)+SUM(minus_2_week_event_count)+SUM(minus_3_week_event_count)+SUM(minus_4_week_event_count) AS l4w_event_count
    FROM
        relative_variation_pre_synchro_1
    GROUP BY
        date,
        event_name,
        website
),

-- '_sd_l4w' for 'same day last 4 weeks'
relative_variation_pre_synchro_3 AS (
    SELECT
        date,
        event_name,
        website,
        SUM(l4w_event_count) AS l4w_event_count,
        (SUM(monitored_event_count)-(SUM(l4w_event_count)/4))/(SUM(l4w_event_count)/4) AS variation_sd_l4w,
        CASE
            WHEN SUM(monitored_event_count) < 0.65*(SUM(l4w_event_count)/4) THEN True
            ELSE False
        END AS greater_35_perc_variation_sd_l4w
    FROM
        relative_variation_pre_synchro_2
    GROUP BY
        date,
        event_name,
        website        
),

relative_variation_pre_synchro_4 AS (
    SELECT
        *,
        LAG(greater_35_perc_variation_sd_l4w) OVER (PARTITION BY event_name, website ORDER BY date ASC) AS minus_1_greater_35_perc_variation_sd_l4w,
        LAG(greater_35_perc_variation_sd_l4w, 2) OVER (PARTITION BY event_name, website ORDER BY date ASC) AS minus_2_greater_35_perc_variation_sd_l4w,
        LAG(greater_35_perc_variation_sd_l4w, 3) OVER (PARTITION BY event_name, website ORDER BY date ASC) AS minus_3_greater_35_perc_variation_sd_l4w,
        LAG(greater_35_perc_variation_sd_l4w, 4) OVER (PARTITION BY event_name, website ORDER BY date ASC) AS minus_4_greater_35_perc_variation_sd_l4w,
        LAG(greater_35_perc_variation_sd_l4w, 5) OVER (PARTITION BY event_name, website ORDER BY date ASC) AS minus_5_greater_35_perc_variation_sd_l4w,
        LAG(greater_35_perc_variation_sd_l4w, 6) OVER (PARTITION BY event_name, website ORDER BY date ASC) AS minus_6_greater_35_perc_variation_sd_l4w,
        LAG(greater_35_perc_variation_sd_l4w, 7) OVER (PARTITION BY event_name, website ORDER BY date ASC) AS minus_7_greater_35_perc_variation_sd_l4w
    FROM
        relative_variation_pre_synchro_3
),

synchro_zero_event AS (
    SELECT
        l.alert_type,
        a.event_name,
        a.website,
        l.last_alert_timestamp,
        DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) AS monitored_date,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) THEN e.m_events ELSE 0 END) AS minus_1_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 2 DAY) THEN e.m_events ELSE 0 END) AS minus_2_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 3 DAY) THEN e.m_events ELSE 0 END) AS minus_3_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 4 DAY) THEN e.m_events ELSE 0 END) AS minus_4_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 5 DAY) THEN e.m_events ELSE 0 END) AS minus_5_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 6 DAY) THEN e.m_events ELSE 0 END) AS minus_6_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 7 DAY) THEN e.m_events ELSE 0 END) AS minus_7_event_count,
        SUM(CASE WHEN e.date = DATE_SUB(CURRENT_DATE(),INTERVAL 8 DAY) THEN e.m_events ELSE 0 END) AS minus_8_event_count
    FROM
        all_events_zero_event a
    LEFT JOIN
        events_scope e
    ON
        e.event_name = a.event_name
        AND e.site_id = a.site_id
    LEFT JOIN
        last_alert l
    ON
        l.event_name = a.event_name
        AND l.website = a.website
        AND l.alert_type = 'zero_event'
    GROUP BY
        alert_type,
        event_name,
        website,
        last_alert_timestamp
    HAVING
        minus_1_event_count = 0
),

synchro_relative_variation AS (
    SELECT
        l.alert_type,
        e.event_name,
        e.website,
        l.last_alert_timestamp,
        DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY) AS monitored_date,
        l4w_event_count,
        variation_sd_l4w,
        greater_35_perc_variation_sd_l4w,
        minus_1_greater_35_perc_variation_sd_l4w,
        minus_2_greater_35_perc_variation_sd_l4w,
        minus_3_greater_35_perc_variation_sd_l4w,
        minus_4_greater_35_perc_variation_sd_l4w,
        minus_5_greater_35_perc_variation_sd_l4w,
        minus_6_greater_35_perc_variation_sd_l4w,
        minus_7_greater_35_perc_variation_sd_l4w
    FROM
        relative_variation_pre_synchro_4 e
    LEFT JOIN
        last_alert l
    ON
        l.event_name = e.event_name
        AND l.website = e.website
        AND l.alert_type = 'relative_variation'
    WHERE
        date = DATE_SUB(CURRENT_DATE(),INTERVAL 1 DAY)
        AND
        greater_35_perc_variation_sd_l4w
)

SELECT
    'zero_event' AS alert_type,
    synchro_zero_event.event_name,
    CAST(NULL AS STRING) AS metric_name,
    CAST(NULL AS ARRAY<STRING>) AS event_parameters,
    CAST(NULL AS STRING) AS segment,    
    synchro_zero_event.website,
    synchro_zero_event.monitored_date AS start_date,
    synchro_zero_event.monitored_date AS end_date,
    CAST(NULL AS DATE) AS compared_start_date,
    CAST(NULL AS DATE) AS compared_end_date,
    STRUCT(
        CAST(NULL AS FLOAT64) AS threshold_decrease,
        CAST(NULL AS FLOAT64) AS threshold_increase,
        CAST(NULL AS FLOAT64) AS current_variation
    ) AS relative_variation,
    STRUCT(
        CAST(NULL AS FLOAT64) AS threshold_min,
        CAST(NULL AS FLOAT64) AS threshold_max,
        CAST(NULL AS FLOAT64) AS current_value
    ) AS absolute_threshold,    
    CURRENT_TIMESTAMP() AS _ingestion_timestamp,
    True AS is_new_alert,
    True AS forced_notification,
    CAST(NULL AS TIMESTAMP) AS previous_alert_timestamp,
    CURRENT_TIMESTAMP() AS first_alert_timestamp,
    CONCAT("L\'événement ",event_name," a été collecté ",minus_1_event_count," fois sur la journée du ",monitored_date) AS rationale
FROM
    synchro_zero_event
    LEFT JOIN
        ${ref("fct_fr_public_holidays")} h
    ON
        h.holiday_date = synchro_zero_event.monitored_date      
WHERE
    synchro_zero_event.minus_1_event_count = 0
    AND synchro_zero_event.minus_2_event_count > 0
    AND
    (
        DATE_DIFF(DATE(CURRENT_TIMESTAMP()), DATE(last_alert_timestamp), DAY) >= 1
        OR
        last_alert_timestamp IS NULL
    )
    AND
    (
        synchro_zero_event.website NOT LIKE '%B2B%'
        OR
        (
            synchro_zero_event.website LIKE '%B2B%'
            AND
            EXTRACT(DAYOFWEEK FROM synchro_zero_event.monitored_date) NOT IN (1,7)
            AND
            h.holiday_name IS NULL            
        )
    )    

UNION ALL

SELECT
    'zero_event' AS alert_type,
    synchro_zero_event.event_name,
    CAST(NULL AS STRING) AS metric_name,
    CAST(NULL AS ARRAY<STRING>) AS event_parameters,
    CAST(NULL AS STRING) AS segment,
    synchro_zero_event.website,
    synchro_zero_event.monitored_date AS start_date,
    synchro_zero_event.monitored_date AS end_date,
    CAST(NULL AS DATE) AS compared_start_date,
    CAST(NULL AS DATE) AS compared_end_date,
    STRUCT(
        CAST(NULL AS FLOAT64) AS threshold_decrease,
        CAST(NULL AS FLOAT64) AS threshold_increase,
        CAST(NULL AS FLOAT64) AS current_variation
    ) AS relative_variation,
    STRUCT(
        CAST(NULL AS FLOAT64) AS threshold_min,
        CAST(NULL AS FLOAT64) AS threshold_max,
        CAST(NULL AS FLOAT64) AS current_value
    ) AS absolute_threshold,    
    CURRENT_TIMESTAMP() AS _ingestion_timestamp,
    False AS is_new_alert,
    True AS forced_notification,
    last_alert.last_alert_timestamp AS previous_alert_timestamp,
    first_alert_timestamp,
    CONCAT("L\'événement ",event_name," a été collecté ",minus_1_event_count," fois sur la journée du ",monitored_date) AS rationale
FROM
    synchro_zero_event
    LEFT JOIN
        last_alert
    USING
        (alert_type, event_name, website)
    LEFT JOIN
        ${ref("fct_fr_public_holidays")} h
    ON
        h.holiday_date = synchro_zero_event.monitored_date          
WHERE
    synchro_zero_event.minus_1_event_count = 0
    AND synchro_zero_event.minus_2_event_count = 0
    AND synchro_zero_event.minus_3_event_count = 0
    AND synchro_zero_event.minus_4_event_count = 0
    AND synchro_zero_event.minus_5_event_count = 0
    AND synchro_zero_event.minus_6_event_count = 0
    AND synchro_zero_event.minus_7_event_count = 0
    AND synchro_zero_event.minus_8_event_count = 0
    AND DATE_DIFF(DATE(CURRENT_TIMESTAMP()), DATE(last_alert.last_alert_timestamp), DAY) >= 7
    AND
    (
        synchro_zero_event.website NOT LIKE '%B2B%'
        OR
        (
            synchro_zero_event.website LIKE '%B2B%'
            AND
            EXTRACT(DAYOFWEEK FROM synchro_zero_event.monitored_date) NOT IN (1,7)
            AND
            h.holiday_name IS NULL            
        )
    )        

UNION ALL

SELECT
    'relative_variation' AS alert_type,
    synchro_relative_variation.event_name,
    CAST(NULL AS STRING) AS metric_name,
    CAST(NULL AS ARRAY<STRING>) AS event_parameters,
    CAST(NULL AS STRING) AS segment,    
    synchro_relative_variation.website,
    synchro_relative_variation.monitored_date AS start_date,
    synchro_relative_variation.monitored_date AS end_date,
    DATE_SUB(synchro_relative_variation.monitored_date,INTERVAL 4 WEEK) AS compared_start_date,
    DATE_SUB(synchro_relative_variation.monitored_date,INTERVAL 1 WEEK) AS compared_end_date,
    STRUCT(
        -0.35 AS threshold_decrease,
        CAST(NULL AS FLOAT64) AS threshold_increase,
        ROUND(variation_sd_l4w,3) AS current_variation
    ) AS relative_variation,
    STRUCT(
        CAST(NULL AS FLOAT64) AS threshold_min,
        CAST(NULL AS FLOAT64) AS threshold_max,
        CAST(NULL AS FLOAT64) AS current_value
    ) AS absolute_threshold,    
    CURRENT_TIMESTAMP() AS _ingestion_timestamp,
    True AS is_new_alert,
    True AS forced_notification,
    CAST(NULL AS TIMESTAMP) AS previous_alert_timestamp,
    CURRENT_TIMESTAMP() AS first_alert_timestamp,
    CONCAT("L\'événement ",event_name," a été collecté ",ROUND(100*ABS(variation_sd_l4w),1),"% fois moins sur la journée du ",monitored_date," vs. en moyenne sur le même jour au cours des 4 dernières semaines.") AS rationale
FROM
    synchro_relative_variation
    LEFT JOIN
        ${ref("fct_fr_public_holidays")} h
    ON
        h.holiday_date = synchro_relative_variation.monitored_date    
WHERE
    synchro_relative_variation.greater_35_perc_variation_sd_l4w
    AND synchro_relative_variation.minus_1_greater_35_perc_variation_sd_l4w = False
    AND
    (
        REGEXP_CONTAINS(synchro_relative_variation.event_name, 'click.action')
        OR
        (
            REGEXP_CONTAINS(synchro_relative_variation.event_name, 'click.action') = False
            AND
            synchro_relative_variation.l4w_event_count/4 >= 3000
        )
    )
    AND
    (
        DATE_DIFF(DATE(CURRENT_TIMESTAMP()), DATE(last_alert_timestamp), DAY) >= 1   
        OR
        last_alert_timestamp IS NULL
    )
    AND
    (
        synchro_relative_variation.website NOT LIKE '%B2B%'
        OR
        (
            synchro_relative_variation.website LIKE '%B2B%'
            AND
            EXTRACT(DAYOFWEEK FROM synchro_relative_variation.monitored_date) NOT IN (1,7)
            AND
            h.holiday_name IS NULL            
        )
    )

UNION ALL

SELECT
    'relative_variation' AS alert_type,
    synchro_relative_variation.event_name,
    CAST(NULL AS STRING) AS metric_name,
    CAST(NULL AS ARRAY<STRING>) AS event_parameters,
    CAST(NULL AS STRING) AS segment,
    synchro_relative_variation.website,
    synchro_relative_variation.monitored_date AS start_date,
    synchro_relative_variation.monitored_date AS end_date,
    DATE_SUB(synchro_relative_variation.monitored_date,INTERVAL 4 WEEK) AS compared_start_date,
    DATE_SUB(synchro_relative_variation.monitored_date,INTERVAL 1 WEEK) AS compared_end_date,
    STRUCT(
      -0.35 AS threshold_decrease,
      CAST(NULL AS FLOAT64) AS threshold_increase,
      ROUND(variation_sd_l4w,3) AS current_variation
    ) AS relative_variation,
    STRUCT(
      CAST(NULL AS FLOAT64) AS threshold_min,
      CAST(NULL AS FLOAT64) AS threshold_max,
      CAST(NULL AS FLOAT64) AS current_value
    ) AS absolute_threshold,    
    CURRENT_TIMESTAMP() AS _ingestion_timestamp,
    False AS is_new_alert,
    True AS forced_notification,
    last_alert.last_alert_timestamp AS previous_alert_timestamp,
    first_alert_timestamp,
    CONCAT("L\'événement ",event_name," a été collecté ",ROUND(100*ABS(variation_sd_l4w),1),"% fois moins sur la journée du ",monitored_date," vs. en moyenne sur le même jour au cours des 4 dernières semaines.") AS rationale
FROM
    synchro_relative_variation
    LEFT JOIN
        last_alert
    USING
        (alert_type, event_name, website)
    LEFT JOIN
        ${ref("fct_fr_public_holidays")} h
    ON
        h.holiday_date = synchro_relative_variation.monitored_date
WHERE
    synchro_relative_variation.greater_35_perc_variation_sd_l4w
    AND synchro_relative_variation.minus_1_greater_35_perc_variation_sd_l4w
    AND synchro_relative_variation.minus_2_greater_35_perc_variation_sd_l4w
    AND synchro_relative_variation.minus_3_greater_35_perc_variation_sd_l4w
    AND synchro_relative_variation.minus_4_greater_35_perc_variation_sd_l4w
    AND synchro_relative_variation.minus_5_greater_35_perc_variation_sd_l4w
    AND synchro_relative_variation.minus_6_greater_35_perc_variation_sd_l4w
    AND synchro_relative_variation.minus_7_greater_35_perc_variation_sd_l4w
    AND
    (
        REGEXP_CONTAINS(synchro_relative_variation.event_name, 'click.action')
        OR
        (
            REGEXP_CONTAINS(synchro_relative_variation.event_name, 'click.action') = False
            AND
            synchro_relative_variation.l4w_event_count/4 >= 3000
        )
    )    
    AND DATE_DIFF(DATE(CURRENT_TIMESTAMP()), DATE(last_alert.last_alert_timestamp), DAY) >= 7
    AND
    (
        synchro_relative_variation.website NOT LIKE '%B2B%'
        OR
        (
            synchro_relative_variation.website LIKE '%B2B%'
            AND
            EXTRACT(DAYOFWEEK FROM synchro_relative_variation.monitored_date) NOT IN (1,7)
            AND
            h.holiday_name IS NULL
        )
    )    
