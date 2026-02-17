SELECT
    FORMAT_DATE('%F', d) as id,
    d AS date_actual,

    EXTRACT(YEAR FROM d) AS year_number,
    DATE_TRUNC(d, YEAR) AS year_trunc,
    CASE 
        WHEN EXTRACT(QUARTER FROM d) IN (1,2,3) 
            THEN CONCAT('exercice ' , EXTRACT(YEAR FROM d) - 1 , '-', EXTRACT(YEAR FROM d) )
        WHEN EXTRACT(QUARTER FROM d) IN (4) 
            THEN CONCAT('exercice ' , EXTRACT(YEAR FROM d) , '-', EXTRACT(YEAR FROM d) + 1 )
        END AS dougs_fiscal_year,

    FORMAT_DATE('%Q', d) as quarter_number,
    CONCAT('Q', FORMAT_DATE('%Q', d)) AS quarter_name,
    DATE_TRUNC(d, QUARTER) AS quarter_trunc,

    EXTRACT(MONTH FROM d) AS month_number,
    FORMAT_DATE('%B', d) as month_name,
    DATE_TRUNC(d, MONTH) AS month_trunc,

    EXTRACT(ISOWEEK FROM d) AS week_number_in_year,
    CONCAT(EXTRACT(YEAR FROM d), ' W', EXTRACT(ISOWEEK FROM d)) AS week_name,
    DATE_TRUNC(d, ISOWEEK) AS week_trunc,

    EXTRACT(DAY FROM d) AS day_number_in_year,
    MOD(EXTRACT(DAYOFWEEK FROM d) + 5, 7) + 1 AS day_number_in_week,   -- car EXTRACT(DAYOFWEEK) commence à 1 = dimanche
    FORMAT_DATE('%A', d) AS day_name,
    (CASE WHEN FORMAT_DATE('%A', d) IN ('Sunday', 'Saturday') THEN FALSE ELSE TRUE END) AS day_is_weekday, --possible de rajouter des exceptions sur jours fériés et autres jours d'exception

    -- Jours fériés France
    (holidays.holiday_date IS NOT NULL) AS is_fr_public_holiday,
    (FORMAT_DATE('%A', d) NOT IN ('Sunday', 'Saturday') AND holidays.holiday_date IS NULL) AS is_fr_business_day

FROM UNNEST(GENERATE_DATE_ARRAY('2015-01-01', '2030-01-01', INTERVAL 1 DAY)) AS d 
LEFT JOIN {{ ref('fct_fr_public_holidays') }} AS holidays ON d = holidays.holiday_date
