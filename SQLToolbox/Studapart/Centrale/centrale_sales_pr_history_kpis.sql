WITH layer_1 AS (
    SELECT
        CASE
            WHEN "{scale}" = "daily" THEN date
            WHEN "{scale}" = "weekly" THEN DATE_TRUNC(date, ISOWEEK)
            WHEN "{scale}" = "monthly" THEN DATE_TRUNC(date, MONTH)
            ELSE DATE("{start_date}")
        END AS from_date,
        CASE
            WHEN "{scale}" = "daily" THEN date
            WHEN "{scale}" = "weekly" THEN LAST_DAY(DATE_TRUNC(date, ISOWEEK), ISOWEEK)
            WHEN "{scale}" = "monthly" THEN LAST_DAY(DATE_TRUNC(date, MONTH), MONTH)
            ELSE DATE("{end_date}")
        END AS to_date,
        CASE WHEN /*@flag_commercialisation_60j = False OR*/ owner_has_flag_commercialisation60j = True THEN account_executive END AS account_executive,
        main_availability_id,
        owner_id,
        CASE WHEN MAX(CASE WHEN owner_has_flag_ambassadeur = True THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_ambassadeur,
        CASE WHEN MAX(CASE WHEN owner_has_flag_midtoppr = True THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_midtoppr,
        CASE WHEN MAX(CASE WHEN owner_has_flag_toppr = True THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_toppr,
        CASE WHEN MAX(CASE WHEN owner_has_flag_bypass = True THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_bypass,
        CASE WHEN MAX(CASE WHEN owner_has_flag_kyc = True THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_kyc,
        CASE WHEN MAX(CASE WHEN owner_has_flag_onboarding = True THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_has_flag_onboarding,
        CASE WHEN MAX(CASE WHEN owner_is_last_login_more_10_days = True THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_is_last_login_more_10_days,
        CASE WHEN MAX(CASE WHEN owner_is_in_negotiation = True THEN 1 ELSE 0 END) = 1 THEN True ELSE False END AS owner_is_in_negotiation,
        MAX(CASE
            WHEN is_request_active AND is_request_active_inprint THEN 1
        END) AS availability_has_active_inprint,
        COUNT(DISTINCT
        CASE
            WHEN is_request_active THEN request_id
        END) AS availability_active_requests_sum,
        COUNT(DISTINCT
        CASE
            WHEN is_request_created THEN request_id
        END) AS availability_requests_sum,
        COUNT(DISTINCT
        CASE
            WHEN is_request_owner_proposition_created THEN request_id
        END) AS availability_active_propositions_sum,
        SUM(owner_action_due_number) AS owner_action_due_number,
        COUNT(DISTINCT
        CASE
            WHEN is_request_active_proposition AND is_request_won THEN request_id
        END) AS availability_won_proposition_requests_sum,
        COUNT(DISTINCT
        CASE
            WHEN is_request_won THEN request_id
        END) AS availability_won_requests_sum,
        SUM(
        CASE
            WHEN is_request_active_proposition AND is_request_won THEN revenues
        END) AS availability_won_proposition_requests_revenues,
        SUM(
        CASE
            WHEN is_request_won THEN revenues
        END) AS availability_won_requests_revenues,
        COUNT(DISTINCT
        CASE
            WHEN is_request_won AND guarantee_margin > 0 THEN request_id
        END) AS availability_won_requests_with_guarantee_sum,
        SUM(
        CASE
            WHEN is_request_active_proposition AND is_request_won THEN guarantee_margin
        END) AS availability_won_proposition_requests_guarantee_margin,
        SUM(
        CASE
            WHEN is_request_won THEN guarantee_margin
        END) AS availability_won_requests_guarantee_margin,
    FROM
        `{project_id}.{analysis_dataset_id}.sales_pr_history_kpis`
    WHERE
        DATE(date) BETWEEN DATE("{start_date}") AND DATE("{end_date}")
        AND DATE(availability_entry_date) BETWEEN DATE_SUB(DATE("{start_date}"), INTERVAL 30 DAY) AND DATE_ADD(DATE("{end_date}"), INTERVAL /*@periode_de_commercialisation*/ 60 DAY)
    GROUP BY
        from_date,
        to_date,
        account_executive,
        main_availability_id,
        owner_id
),

layer_2 AS (
    SELECT
        from_date,
        to_date,
        account_executive,
        owner_id,
        owner_has_flag_ambassadeur,
        owner_has_flag_midtoppr,
        owner_has_flag_toppr,
        owner_has_flag_bypass,
        owner_has_flag_kyc,
        owner_has_flag_onboarding,
        owner_is_last_login_more_10_days,
        owner_is_in_negotiation,
        COUNT(DISTINCT main_availability_id) AS availability_count,
        SUM(owner_action_due_number) AS owner_action_due_number,
        CASE WHEN MAX(availability_has_active_inprint) = 1 THEN True ELSE False END AS owner_has_active_inprint,
        SUM(availability_active_requests_sum) AS owner_total_active_requests,
        -- MAX(availability_active_requests_sum) AS max_availability_active_requests,
        MAX(availability_requests_sum) AS max_availability_requests,
        SUM(availability_won_requests_with_guarantee_sum) AS owner_total_won_requests_with_guarantee,
        MAX(availability_won_requests_with_guarantee_sum) AS max_availability_won_requests_with_guarantee,
        SUM(availability_active_propositions_sum) AS owner_total_active_propositions,
        MAX(availability_active_propositions_sum) AS max_availability_active_propositions,
        SUM(availability_won_proposition_requests_sum) AS owner_total_won_proposition_requests,
        SUM(availability_won_requests_sum) AS owner_total_won_requests,
        SUM(availability_won_proposition_requests_revenues) AS owner_total_won_proposition_requests_revenues,
        SUM(availability_won_requests_revenues) AS owner_total_won_requests_revenues,
        SUM(availability_won_proposition_requests_guarantee_margin) AS owner_total_won_proposition_requests_guarantee_margin,
        SUM(availability_won_requests_guarantee_margin) AS owner_total_won_requests_guarantee_margin,
    FROM
        layer_1
    GROUP BY
        from_date,
        to_date,
        account_executive,
        owner_id,
        owner_has_flag_ambassadeur,
        owner_has_flag_midtoppr,
        owner_has_flag_toppr,
        owner_has_flag_bypass,
        owner_has_flag_kyc,
        owner_has_flag_onboarding,
        owner_is_last_login_more_10_days,
        owner_is_in_negotiation
)

SELECT
    from_date,
    to_date,
    account_executive,
    COUNT(DISTINCT owner_id) AS pr_dispo_n_jours,
    COUNT(DISTINCT
        CASE
        WHEN max_availability_requests = 0 THEN owner_id
    END) AS pr_0_demande,
    COUNT(DISTINCT
        CASE
        WHEN owner_total_active_requests >= /*@seuil_activation*/ 5 THEN owner_id
    END) AS pr_actives,
    COUNT(DISTINCT
        CASE
        WHEN owner_total_active_requests < /*@seuil_activation*/ 5 THEN owner_id
    END) AS pr_non_actives,
    COUNT(DISTINCT
        CASE
        WHEN owner_total_active_propositions > 0 THEN owner_id
    END) AS pr_min_1_da,
    COUNT(DISTINCT
        CASE
        WHEN owner_total_active_requests >= /*@seuil_activation*/ 5 AND owner_total_active_propositions > 0 THEN owner_id
    END) AS pr_actives_min_1_da,
    COUNT(DISTINCT
        CASE
        WHEN owner_has_flag_kyc = TRUE THEN owner_id
    END) AS pr_non_kyc,
    COUNT(DISTINCT
        CASE
        WHEN owner_has_flag_onboarding = FALSE THEN owner_id
    END) AS pr_non_onboarde,
    COUNT(DISTINCT
        CASE
        WHEN owner_has_flag_bypass = TRUE THEN owner_id
    END) AS pr_bypass,
    COUNT(DISTINCT
        CASE
        WHEN owner_is_last_login_more_10_days = True THEN owner_id
    END) AS pr_connexion_plus_de_10_jours,
    COUNT(DISTINCT
        CASE
        WHEN owner_total_active_requests >= /*@seuil_activation*/ 5 AND owner_is_last_login_more_10_days = True THEN owner_id
    END) AS pr_actives_connexion_plus_de_10_jours,
    COUNT(DISTINCT
        CASE
        WHEN owner_action_due_number > 0 THEN owner_id
    END) AS pr_actions_due_late,
    COUNT(DISTINCT
        CASE
        WHEN owner_is_in_negotiation = TRUE THEN owner_id
    END) AS pr_in_negotiation,
    SUM(owner_total_won_requests) AS bookings,
    SUM(owner_total_won_requests_with_guarantee) AS bookings_with_guarantee,
    COUNT(DISTINCT
        CASE
        WHEN owner_has_active_inprint = TRUE THEN owner_id
    END) AS pr_inprints,
    SUM(owner_total_won_requests_revenues) AS bookings_revenues,
    ROUND(SAFE_DIVIDE(SUM(owner_total_won_requests),SUM(availability_count)),4) AS taux_de_conversion_dispo,
    ROUND(SAFE_DIVIDE(SUM(owner_total_won_proposition_requests),SUM(owner_total_active_propositions)),4) AS taux_de_conversion_da,
    COUNT(DISTINCT
        CASE
        WHEN owner_has_flag_ambassadeur = TRUE THEN owner_id
    END) AS pr_ambassadeur,    
    COUNT(DISTINCT
        CASE
        WHEN owner_has_flag_toppr = TRUE THEN owner_id
    END) AS top_pr,
    COUNT(DISTINCT
        CASE
        WHEN owner_has_flag_midtoppr = TRUE THEN owner_id
    END) AS mid_top_pr
FROM
    layer_2
GROUP BY
    from_date,
    to_date,
    account_executive
