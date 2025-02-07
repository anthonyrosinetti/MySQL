WITH date_range AS (
    SELECT
        DATE(date) AS date,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(DATE("{start_date}"), DATE("{end_date}"), INTERVAL 1 DAY)) AS date
    WHERE
        DATE(date) >= "2024-12-20"
),

-- Owner flags
flag_history_l1 AS (
    SELECT DISTINCT
        uf.user_id AS owner_id,
        f.name AS flag_name,
        DATE(uf.created_at) AS created_at,
        LAST_VALUE(DATE(uf.removed_at) IGNORE NULLS) OVER w AS removed_at,
    FROM
        `{project_id}.{db_dataset_id}.postgres_users_flags_history_changes` uf
    LEFT JOIN
        `{project_id}.{db_dataset_id}.postgres_flags_raw_data` f
    ON
        uf.flag_id = f.id
    WHERE
        f.name IN ("Bypass", "Ambassadeur", "Mid-top PR 2024", "TOP PR 2024", "KYC", "ONBOARDING_ACCOUNT", "Commercialisation -60J")
    WINDOW
        w AS (
            PARTITION BY uf.user_id, f.name, DATE(uf.created_at)
            ORDER BY uf.ingestion_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),

flag_history_l2 AS (
    SELECT
        date_range.date,
        f.owner_id,
        CASE WHEN MAX(CASE WHEN f.flag_name = "Bypass" THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_bypass,
        CASE WHEN MAX(CASE WHEN f.flag_name = "Ambassadeur" THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_ambassadeur,
        CASE WHEN MAX(CASE WHEN f.flag_name = "Mid-top PR 2024" THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_midtoppr,
        CASE WHEN MAX(CASE WHEN f.flag_name = "TOP PR 2024" THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_toppr,
        CASE WHEN MAX(CASE WHEN f.flag_name = "KYC" THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_kyc,
        CASE WHEN MAX(CASE WHEN f.flag_name = "ONBOARDING_ACCOUNT" THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_onboarding,
        CASE WHEN MAX(CASE WHEN f.flag_name = "Commercialisation -60J" THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_commercialisation60j,
    FROM
        flag_history_l1 AS f
    LEFT JOIN
        date_range
    ON
        date_range.date >= f.created_at
        AND date_range.date <= IFNULL(f.removed_at, DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))
    GROUP BY
        date,
        owner_id
),

-- User last login
user_login_history_l1 AS (
    SELECT
        id AS owner_id,
        DATE(last_login_date) AS last_login_date,
        MIN(ingestion_timestamp) AS ingestion_timestamp,
    FROM
        `{project_id}.{db_dataset_id}.postgres_users_history_changes`
    WHERE
        ARRAY_TO_STRING(roles, "/") = "ROLE_OWNER"
    GROUP BY
        owner_id,
        last_login_date
),

user_login_history_l2 AS (
    SELECT DISTINCT
        ulh.owner_id,
        date_range.date,
        LAST_VALUE(last_login_date IGNORE NULLS) OVER w AS last_login_date,
    FROM
        user_login_history_l1 AS ulh
    CROSS JOIN
        date_range
    WHERE
        DATE(ulh.ingestion_timestamp) <= date_range.date
        AND date_range.date >= DATE("2024-12-10") -- Start of history
    WINDOW
        w AS (
            PARTITION BY owner_id, date
            ORDER BY ingestion_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),
-- User action dues
user_action_dues_l1 AS (
    SELECT DISTINCT
        id AS action_id,
        type AS action_type,
        user_id AS owner_id,
        DATE(MIN(created_at) OVER w) AS created_date,
        DATE(LAST_VALUE(due_at IGNORE NULLS) OVER w) AS due_at,
        DATE(MIN(CASE WHEN status IN ("done", "canceled") THEN LEAST(ingestion_timestamp, IFNULL(updated_at, created_at)) END) OVER w) AS completion_date
    FROM
        `{project_id}.{db_dataset_id}.postgres_actions_history_changes`
    WINDOW
        w AS (
            PARTITION BY id, user_id
            ORDER BY ingestion_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),

user_action_dues_l2 AS (
    SELECT
        uad.owner_id,
        date_range.date,
        COUNT(DISTINCT CASE
            WHEN date_range.date >= uad.due_at THEN action_id
        END) AS action_due_number,
        CASE WHEN MAX(CASE
            WHEN uad.action_type = "proposition_negotiation" AND date_range.date >= uad.created_date THEN 1
            ELSE 0
        END) = 1 THEN True ELSE False END AS is_in_negotiation        
    FROM
        user_action_dues_l1 AS uad
    CROSS JOIN
        date_range
    WHERE
        (date_range.date <= uad.completion_date OR uad.completion_date IS NULL) -- All data after completion date is not useful
    GROUP BY
        owner_id,
        date
),

-- Request Status History
request_status_history_l1 AS (
    SELECT
        user_id,
        id AS request_id,
        created_at,
        status,
        ingestion_timestamp AS status_timestamp,
        LAG(status) OVER (PARTITION BY id ORDER BY ingestion_timestamp ASC) AS previous_status,
    FROM
        `{project_id}.{db_dataset_id}.postgres_requests_history_changes`
),

request_status_history_l2 AS (
    SELECT
        * EXCEPT(previous_status),
        CASE WHEN LOWER(status) LIKE "%canceled%" OR LOWER(status) LIKE "%refused%" THEN False ELSE True END AS is_request_active,
    FROM
        request_status_history_l1
    WHERE
        previous_status IS NULL
        OR status != previous_status
),

request_active_dates AS (
    SELECT DISTINCT
        request_id,
        DATE(MIN(created_at) OVER w) AS created_at,
        DATE(FIRST_VALUE(CASE WHEN is_request_active = False THEN status_timestamp END IGNORE NULLS) OVER w) AS inactive_at,
    FROM
        request_status_history_l2
    WINDOW
        w AS (
            PARTITION BY request_id
            ORDER BY status_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),

-- Account Executive attribution
account_executive_attribution AS (
    SELECT
        user_id AS owner_id,
        account_executive.id AS account_executive_id,
        CONCAT(account_executive.first_name, " ", account_executive.last_name) AS account_executive,
        DATE(attribution_start) AS attribution_start,
        DATE(exact_attribution_end) AS attribution_end,
    FROM
        `{project_id}.{db_dataset_id}.preanalysis_db_users_account_executive_attribution`
),

-- Avail & Requests
avail_requests AS (
    SELECT
        main_availability_id,
        availability_id,
        request_id,
        DATE(availability_configuration.date_start) AS availability_entry_date,
        DATE(availability_dates.created_at) AS availability_created_date,
        owner_id,
        DATE(request_dates.created_at) AS request_created_date,
        DATE(request_won_at) AS request_won_at,
        request_status,
        request_has_card_inprint,
        DATE(request_owner_proposition_created_at) AS request_owner_proposition_created_at,
        request_has_owner_proposition,
        mobility_id,
        DATE(mobility_dates.won_at) AS mobility_won_at,
        last_payment.fees_amount AS revenues,
        last_payment.guarantee_amount AS guarantee_margin
    FROM
        `{project_id}.{db_dataset_id}.preanalysis_db_availabilities_requests`
    WHERE
        owner_role.roles = "ROLE_OWNER"
        AND availability_id IS NOT NULL
),

availabilities AS (
    SELECT DISTINCT
        main_availability_id,
        availability_id,
        availability_entry_date,
        availability_created_date,
        owner_id,
    FROM
        avail_requests
),

requests AS (
    SELECT DISTINCT
        date_range.date,
        ar.main_availability_id,
        ar.availability_id,
        ar.request_id,
        LEAST(ar.request_created_date, rad.created_at) AS request_created_date,
        ar.request_won_at,
        ar.request_status,
        ar.request_has_card_inprint,
        ar.request_owner_proposition_created_at,
        ar.request_has_owner_proposition,
        ar.mobility_id,
        CASE WHEN ar.mobility_won_at = date_range.date THEN ar.mobility_won_at END AS mobility_won_at,
        CASE WHEN ar.request_won_at = date_range.date THEN ar.revenues END AS revenues,
        CASE WHEN ar.request_won_at = date_range.date THEN ar.guarantee_margin END AS guarantee_margin,
        rad.inactive_at AS request_inactive_date,
    FROM
        avail_requests AS ar
    LEFT JOIN
        request_active_dates AS rad
    USING
        (request_id)
    CROSS JOIN
        date_range
    WHERE
        date_range.date >= LEAST(ar.request_created_date, rad.created_at)
        AND (
            (ar.request_won_at IS NOT NULL AND date_range.date <= ar.request_won_at) -- Won requests
            OR (ar.request_won_at IS NULL AND rad.inactive_at IS NOT NULL AND date_range.date <= rad.inactive_at) -- Canceled/Refused requests
            OR (ar.request_won_at IS NULL AND rad.inactive_at IS NULL) -- Active requests
        )
),

availability_online_dates AS ( -- One line per main-availability & online date of the availability
    SELECT DISTINCT
        DATE(od.date_online) AS date,
        availabilities.main_availability_id,
        availabilities.availability_id,
        availabilities.availability_entry_date,
        availabilities.availability_created_date,
        availabilities.owner_id,
        aea.account_executive_id,
        aea.account_executive,
    FROM
        `{project_id}.{db_dataset_id}.preanalysis_db_availabilities_online_dates` AS aod,
        UNNEST(online_dates) AS od
    INNER JOIN -- Remove availabilities that are not from owners
        availabilities
    USING
        (availability_id)
    INNER JOIN -- Keep only the date in the date range we are intetrested in
        date_range
    ON
        date_range.date = DATE(od.date_online)
    LEFT JOIN -- Adding account executive attributed for the given day
        account_executive_attribution AS aea
    ON
        aea.owner_id = availabilities.owner_id
        AND DATE(od.date_online) BETWEEN aea.attribution_start AND IFNULL(aea.attribution_end, DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY))
),

availabliity_requests_online_dates AS (
    SELECT
        aod.date,
        aod.main_availability_id,
        aod.availability_id,
        aod.availability_entry_date,
        aod.availability_created_date,
        aod.owner_id,
        aod.account_executive_id,
        aod.account_executive,
        req.request_id,
        req.request_created_date,
        req.request_won_at,
        req.request_status,
        req.request_has_card_inprint,
        req.request_owner_proposition_created_at,
        req.request_has_owner_proposition,
        req.mobility_id,
        req.mobility_won_at,
        req.revenues,
        req.guarantee_margin,
        req.request_inactive_date,
    FROM
        availability_online_dates AS aod
    LEFT JOIN
        requests AS req
    ON
        aod.availability_id = req.availability_id
        AND aod.date = req.date
)

SELECT
    ar.date,
    ar.main_availability_id,
    ar.availability_id,
    ar.availability_entry_date,
    ar.owner_id,
    IFNULL(f.owner_has_flag_bypass, False) AS owner_has_flag_bypass,
    IFNULL(f.owner_has_flag_ambassadeur, False) AS owner_has_flag_ambassadeur,
    IFNULL(f.owner_has_flag_midtoppr, False) AS owner_has_flag_midtoppr,
    IFNULL(f.owner_has_flag_toppr, False) AS owner_has_flag_toppr,
    IFNULL(f.owner_has_flag_kyc, False) AS owner_has_flag_kyc,
    IFNULL(f.owner_has_flag_onboarding, False) AS owner_has_flag_onboarding,
    IFNULL(f.owner_has_flag_commercialisation60j, False) AS owner_has_flag_commercialisation60j,
    log.last_login_date AS owner_last_login_date,
    CASE WHEN DATE_DIFF(ar.date, log.last_login_date, DAY) > 10 THEN True ELSE False END AS owner_is_last_login_more_10_days,
    act.action_due_number AS owner_action_due_number,
    act.is_in_negotiation AS owner_is_in_negotiation,
    ar.account_executive_id,
    ar.account_executive,
    ar.request_id,
    CASE WHEN ar.request_won_at = ar.date THEN True WHEN ar.request_id IS NOT NULL THEN False END AS is_request_won,
    CASE WHEN ar.request_won_at = ar.date THEN ar.revenues END AS revenues,
    CASE WHEN ar.request_won_at = ar.date THEN ar.guarantee_margin END AS guarantee_margin,
    CASE WHEN ar.request_has_card_inprint = True AND ar.date >= ar.request_created_date AND (ar.date <= ar.request_inactive_date OR ar.request_inactive_date IS NULL) THEN True WHEN ar.request_id IS NOT NULL THEN False END AS is_request_active_inprint,
    CASE WHEN ar.date = ar.request_owner_proposition_created_at AND (ar.date <= ar.request_inactive_date OR ar.request_inactive_date IS NULL) AND (ar.date <= ar.request_won_at OR ar.request_won_at IS NULL) THEN True WHEN ar.request_id IS NOT NULL THEN False END AS is_request_owner_proposition_created,
    CASE WHEN ar.date >= ar.request_created_date AND (ar.date <= ar.request_inactive_date OR ar.request_inactive_date IS NULL) AND (ar.date <= ar.request_won_at OR ar.request_won_at IS NULL) THEN True WHEN ar.request_id IS NOT NULL THEN False END AS is_request_active,
    CASE WHEN ar.date = ar.request_created_date THEN True ELSE False END AS is_request_created,    
    ar.mobility_id,
    CASE WHEN ar.mobility_won_at = ar.date THEN True WHEN ar.mobility_id IS NOT NULL THEN False END AS is_mobility_won,
FROM
    availabliity_requests_online_dates AS ar
LEFT JOIN
    flag_history_l2 AS f
USING
    (date, owner_id)
LEFT JOIN
    user_login_history_l2 AS log
USING
    (date, owner_id)
LEFT JOIN
    user_action_dues_l2 AS act
USING
    (date, owner_id)
