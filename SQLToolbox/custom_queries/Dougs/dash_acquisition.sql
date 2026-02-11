{{ config(
    materialized="table",
    partition_by={
        "field": "date_cohort",
        "data_type": "date",
        "granularity": "day"
    }
) }}

{%- set date_query -%}
    SELECT 
        MIN(date_actual) as min_date, 
        MAX(date_actual) as max_date 
    FROM {{ ref('objectives_global_monthly') }} o 
    LEFT JOIN {{ ref('dim_dates') }} d USING (month_trunc)
{%- endset -%}

{%- set results = run_query(date_query) -%}

{%- if execute -%}
    {# On extrait les valeurs du premier enregistrement (index 0) #}
    {%- set min_date = results.columns[0].values()[0] -%}
    {%- set max_date = results.columns[1].values()[0] -%}
{%- else -%}
    {# Valeurs par défaut pour la phase de compilation #}
    {%- set min_date = '2024-10-01' -%}
    {%- set max_date = '2026-09-30' -%}
{%- endif -%}

WITH months_days AS (
  SELECT DISTINCT
        o.month_trunc,
        COUNT(d.date_actual) AS days_count
  FROM
        {{ ref('objectives_global_monthly') }} o
  LEFT JOIN
        {{ ref('dim_dates') }} d
  USING
        (month_trunc)
  GROUP BY
        month_trunc
),

daily_goals AS (
    SELECT
        date,
        ROUND(SAFE_DIVIDE(obj_nb_companies_acquired,days_count),0) AS accountings_goal,
        ROUND(SAFE_DIVIDE(obj_creation_to_accounting,days_count),0) AS crea_accounting_transitions_goal,
        ROUND(SAFE_DIVIDE(obj_facturation_to_accounting,days_count),0) AS invoicing_accounting_transitions_goal,
--        ROUND(SAFE_DIVIDE(obj_nb_companies_acquired-obj_creation_to_accounting-obj_invoicing_to_accounting,days_count),0) AS direct_accountings_goal,
        ROUND(SAFE_DIVIDE(obj_accounting_direct,days_count),0) AS direct_accountings_goal,
        ROUND(SAFE_DIVIDE(obj_quote_2_accepted,days_count),0) AS creations_goal,
        ROUND(SAFE_DIVIDE(obj_quote_1_accepted,days_count),0) AS opportunities_goal,
        ROUND(SAFE_DIVIDE(obj_mql,days_count),0) AS mqls_goal,
        ROUND(SAFE_DIVIDE(obj_lead,days_count),0) AS leads_goal,
        ROUND(SAFE_DIVIDE(obj_budget_marketing_paid,days_count),0) AS spend_goal,
        ROUND(SAFE_DIVIDE(obj_MRR_accounting,days_count),2) AS accounting_revenues_goal
    FROM
        UNNEST(GENERATE_DATE_ARRAY('{{ min_date }}','{{ max_date }}')) date
        LEFT JOIN
            {{ ref('objectives_global_monthly') }} o
        ON
            o.month_trunc = DATE_TRUNC(date,MONTH)
        LEFT JOIN
            months_days md
        ON
            md.month_trunc = DATE_TRUNC(date,MONTH)
),

indicators_from_all_sources AS (
    SELECT
        contact_id,
        dougs_user_id,
        company_id,
        dougs_company_id,
        gender,
        age_range,
        eligible,
        legal_form,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        first_page,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        pre_conversion_touchpoints,
        pre_conversion_forms,
        model_type,    
        CASE
            WHEN model_type = "first_touch" THEN first_touchpoint_source_name
            WHEN model_type = "first_form" THEN first_form_source_name
            WHEN model_type = "lead_last_touch" THEN lead_last_touchpoint_source_name
            WHEN model_type = "mql_last_touch" THEN mql_last_touchpoint_source_name
        END AS source_name,
        CASE
            WHEN model_type = "first_touch" THEN first_touchpoint_channel
            WHEN model_type = "first_form" THEN first_form_channel
            WHEN model_type = "lead_last_touch" THEN lead_last_touchpoint_channel
            WHEN model_type = "mql_last_touch" THEN mql_last_touchpoint_channel
        END AS channel,
        CASE
            WHEN model_type = "first_touch" THEN first_touchpoint_campaign_group
            WHEN model_type = "first_form" THEN first_form_campaign_group
            WHEN model_type = "lead_last_touch" THEN lead_last_touchpoint_campaign_group
            WHEN model_type = "mql_last_touch" THEN mql_last_touchpoint_campaign_group
        END AS campaign_group, 
        CASE
            WHEN model_type = "first_touch" THEN first_touchpoint_term
            WHEN model_type = "first_form" THEN first_form_term
            WHEN model_type = "lead_last_touch" THEN lead_last_touchpoint_term
            WHEN model_type = "mql_last_touch" THEN mql_last_touchpoint_term
        END AS term,               
        pack_choice,
        CAST(NULL AS STRING) AS prestation_name,
        CAST(NULL AS STRING) AS prestation_category,
        CAST(NULL AS STRING) AS data_prestation_category,
        CASE
            WHEN calculation_type LIKE "%_by_lead_date" THEN date_lead
            WHEN calculation_type LIKE "%_by_mql_date" THEN date_mql
            WHEN calculation_type LIKE "%_by_opportunity_date" THEN date_opportunity
            WHEN calculation_type LIKE "%_by_signup_invoicing_date" THEN date_signup_invoicing
            WHEN calculation_type LIKE "%_by_won_invoicing_date" THEN date_won_invoicing
            WHEN calculation_type LIKE "%_by_won_creation_date" THEN date_won_creation
            WHEN calculation_type LIKE "%_by_won_accounting_date" THEN date_won_accounting
            WHEN calculation_type LIKE "%_by_lost_date" THEN date_lost
            WHEN calculation_type LIKE "%_by_reactivation_date" THEN LEAST(date_won_invoicing,date_won_creation,date_won_accounting)
        END AS date_cohort,
        CASE
            WHEN calculation_type LIKE "leads_by_%" THEN date_lead
            WHEN calculation_type LIKE "mqls_by_%" THEN date_mql
            WHEN calculation_type LIKE "opportunities_by_%" THEN date_opportunity
            WHEN calculation_type LIKE "signups_invoicing_by_%" THEN date_signup_invoicing
            WHEN calculation_type LIKE "clients_invoicing_by_%" THEN date_won_invoicing
            WHEN calculation_type LIKE "clients_creation_by_%" THEN date_won_creation
            WHEN calculation_type LIKE "clients_accounting_by_%" THEN date_won_accounting
            WHEN calculation_type LIKE "clients_direct_accounting_by_%" THEN date_won_accounting
            WHEN calculation_type LIKE "clients_invoicing_to_accounting_by_%" THEN date_won_accounting
            WHEN calculation_type LIKE "clients_crea_to_accounting_by_%" THEN date_won_invoicing
            WHEN calculation_type LIKE "losts_by_%" THEN date_lost
            WHEN calculation_type LIKE "reactivations_by_%" THEN LEAST(date_won_invoicing,date_won_creation,date_won_accounting)
        END AS date_actual,
        CASE WHEN calculation_type = "leads_by_lead_date" AND date_lead IS NOT NULL THEN 1 END AS leads_by_lead_date,
        CASE WHEN calculation_type = "mqls_by_lead_date" AND date_mql IS NOT NULL THEN 1 END AS mqls_by_lead_date,
        CASE WHEN calculation_type = "mqls_by_signup_invoicing_date" AND date_mql IS NOT NULL THEN 1 END AS mqls_by_signup_invoicing_date,
        CASE WHEN calculation_type = "mqls_by_mql_date" AND date_mql IS NOT NULL THEN 1 END AS mqls_by_mql_date,
        CASE WHEN calculation_type = "opportunities_by_mql_date" AND date_opportunity IS NOT NULL THEN 1 END AS opportunities_by_mql_date,
        CASE WHEN calculation_type = "opportunities_by_opportunity_date" AND date_opportunity IS NOT NULL THEN 1 END AS opportunities_by_opportunity_date,
        CASE WHEN calculation_type = "signups_invoicing_by_signup_invoicing_date" AND date_signup_invoicing IS NOT NULL THEN 1 END AS signups_invoicing_by_signup_invoicing_date,
        -- CASE WHEN calculation_type = "clients_by_lead_date" AND date_won IS NOT NULL THEN 1 END AS clients_by_lead_date,
        -- CASE WHEN calculation_type = "clients_by_mql_date" AND date_won IS NOT NULL THEN 1 END AS clients_by_mql_date,
        -- CASE WHEN calculation_type = "clients_by_won_date" AND date_won IS NOT NULL THEN 1 END AS clients_by_won_date,
        CASE WHEN calculation_type = "clients_invoicing_by_lead_date" AND date_won_invoicing IS NOT NULL THEN 1 END AS clients_invoicing_by_lead_date,
        CASE WHEN calculation_type = "clients_invoicing_by_mql_date" AND date_won_invoicing IS NOT NULL THEN 1 END AS clients_invoicing_by_mql_date,
        CASE WHEN calculation_type = "clients_invoicing_by_won_invoicing_date" AND date_won_invoicing IS NOT NULL THEN 1 END AS clients_invoicing_by_won_date,
        CASE WHEN calculation_type = "clients_creation_by_lead_date" AND date_won_creation IS NOT NULL THEN 1 END AS clients_creation_by_lead_date,
        CASE WHEN calculation_type = "clients_creation_by_mql_date" AND date_won_creation IS NOT NULL THEN 1 END AS clients_creation_by_mql_date,
        CASE WHEN calculation_type = "clients_creation_by_opportunity_date" AND date_won_creation IS NOT NULL THEN 1 END AS clients_creation_by_opportunity_date,
        CASE WHEN calculation_type = "clients_creation_by_won_creation_date" AND date_won_creation IS NOT NULL THEN 1 END AS clients_creation_by_won_date,
        CASE WHEN calculation_type = "clients_accounting_by_lead_date" AND date_won_accounting IS NOT NULL THEN 1 END AS clients_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_accounting_by_mql_date" AND date_won_accounting IS NOT NULL THEN 1 END AS clients_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL THEN 1 END AS clients_accounting_by_won_date,
        CASE WHEN calculation_type = "clients_direct_accounting_by_lead_date" AND date_won_accounting IS NOT NULL AND (date_won_invoicing IS NULL OR date_won_invoicing > date_won_accounting) AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN 1 END AS clients_direct_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_direct_accounting_by_mql_date" AND date_won_accounting IS NOT NULL AND (date_won_invoicing IS NULL OR date_won_invoicing > date_won_accounting) AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN 1 END AS clients_direct_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_direct_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL AND (date_won_invoicing IS NULL OR date_won_invoicing > date_won_accounting) AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN 1 END AS clients_direct_accounting_by_won_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_lead_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN 1 END AS clients_invoicing_to_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_signup_invoicing_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN 1 END AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_mql_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN 1 END AS clients_invoicing_to_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN 1 END AS clients_invoicing_to_accounting_by_won_date,           
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_lead_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN 1 END AS clients_crea_to_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_mql_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN 1 END AS clients_crea_to_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_won_creation_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN 1 END AS clients_crea_to_accounting_by_won_creation_date,                
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN 1 END AS clients_crea_to_accounting_by_won_date,                
        CASE WHEN calculation_type = "losts_by_lead_date" AND date_lost IS NOT NULL THEN 1 END AS losts_by_lead_date,
        CASE WHEN calculation_type = "losts_by_lost_date" AND date_lost IS NOT NULL THEN 1 END AS losts_by_lost_date,
        CASE WHEN calculation_type = "reactivations_by_reactivation_date" AND date_lost < LEAST(date_won_invoicing,date_won_creation,date_won_accounting) THEN 1 END AS reactivations_by_reactivation_date,
        CAST(NULL AS FLOAT64) AS prestation_amount,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS sessions,
        CAST(NULL AS INT64) AS accountings_goal,
        CAST(NULL AS INT64) AS crea_accounting_transitions_goal,
        CAST(NULL AS INT64) AS invoicing_accounting_transitions_goal,
        CAST(NULL AS INT64) AS direct_accountings_goal,
        CAST(NULL AS INT64) AS creations_goal,
        CAST(NULL AS INT64) AS opportunities_goal,
        CAST(NULL AS INT64) AS mqls_goal,
        CAST(NULL AS INT64) AS leads_goal,
        CAST(NULL AS INT64) AS spend_goal,
        CAST(NULL AS FLOAT64) AS accounting_revenues_goal
    FROM
        {{ ref('int_attributed_contact_revenues') }} 
    CROSS JOIN
        UNNEST(["leads_by_lead_date",
        "mqls_by_lead_date",
        "mqls_by_signup_invoicing_date",
        "mqls_by_mql_date",
        "opportunities_by_mql_date",
        "opportunities_by_opportunity_date",
        "signups_invoicing_by_signup_invoicing_date",
        "clients_invoicing_by_lead_date",
        "clients_invoicing_by_mql_date",
        "clients_invoicing_by_won_invoicing_date",
        "clients_creation_by_lead_date",
        "clients_creation_by_mql_date",
        "clients_creation_by_opportunity_date",
        "clients_creation_by_won_creation_date",
        "clients_accounting_by_lead_date",
        "clients_accounting_by_mql_date",
        "clients_accounting_by_won_accounting_date",
        "clients_direct_accounting_by_lead_date",
        "clients_direct_accounting_by_mql_date",
        "clients_direct_accounting_by_won_accounting_date",
        "clients_invoicing_to_accounting_by_lead_date",
        "clients_invoicing_to_accounting_by_signup_invoicing_date",
        "clients_invoicing_to_accounting_by_mql_date",
        "clients_invoicing_to_accounting_by_won_accounting_date",
        "clients_crea_to_accounting_by_lead_date",
        "clients_crea_to_accounting_by_mql_date",
        "clients_crea_to_accounting_by_won_creation_date",
        "clients_crea_to_accounting_by_won_accounting_date",
        "losts_by_lead_date",
        "losts_by_lost_date",
        "reactivations_by_reactivation_date"]) AS calculation_type
    CROSS JOIN
        UNNEST(["first_touch",
        "first_form",
        "lead_last_touch",
        "mql_last_touch"
        ]) AS model_type        
    WHERE
        event_type IS NULL

    UNION ALL

    SELECT
        contact_id,
        dougs_user_id,
        company_id,
        dougs_company_id,
        gender,
        age_range,
        eligible,
        legal_form,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        first_page,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        pre_conversion_touchpoints,
        pre_conversion_forms,     
        "lead_linear" AS model_type,
        l.lead_linear_source_name AS source_name,
        l.lead_linear_channel AS channel,
        l.lead_linear_campaign_group AS campaign_group,
        l.lead_linear_utm_term AS term,        
        pack_choice,
        CAST(NULL AS STRING) AS prestation_name,
        CAST(NULL AS STRING) AS prestation_category,
        CAST(NULL AS STRING) AS data_prestation_category,
        CASE
            WHEN calculation_type LIKE "%_by_lead_date" THEN date_lead
            WHEN calculation_type LIKE "%_by_mql_date" THEN date_mql
            WHEN calculation_type LIKE "%_by_opportunity_date" THEN date_opportunity
            WHEN calculation_type LIKE "%_by_signup_invoicing_date" THEN date_signup_invoicing
            WHEN calculation_type LIKE "%_by_won_invoicing_date" THEN date_won_invoicing
            WHEN calculation_type LIKE "%_by_won_creation_date" THEN date_won_creation
            WHEN calculation_type LIKE "%_by_won_accounting_date" THEN date_won_accounting
            WHEN calculation_type LIKE "%_by_lost_date" THEN date_lost
            WHEN calculation_type LIKE "%_by_reactivation_date" THEN LEAST(date_won_invoicing,date_won_creation,date_won_accounting)
        END AS date_cohort,
        CASE
            WHEN calculation_type LIKE "leads_by_%" THEN date_lead
            WHEN calculation_type LIKE "mqls_by_%" THEN date_mql
            WHEN calculation_type LIKE "opportunities_by_%" THEN date_opportunity
            WHEN calculation_type LIKE "signups_invoicing_by_%" THEN date_signup_invoicing
            WHEN calculation_type LIKE "clients_invoicing_by_%" THEN date_won_invoicing
            WHEN calculation_type LIKE "clients_creation_by_%" THEN date_won_creation
            WHEN calculation_type LIKE "clients_accounting_by_%" THEN date_won_accounting
            WHEN calculation_type LIKE "clients_direct_accounting_by_%" THEN date_won_accounting
            WHEN calculation_type LIKE "clients_invoicing_to_accounting_by_%" THEN date_won_accounting
            WHEN calculation_type LIKE "clients_crea_to_accounting_by_%" THEN date_won_invoicing
            WHEN calculation_type LIKE "losts_by_%" THEN date_lost
            WHEN calculation_type LIKE "reactivations_by_%" THEN LEAST(date_won_invoicing,date_won_creation,date_won_accounting)
        END AS date_actual,
        CASE WHEN calculation_type = "leads_by_lead_date" AND date_lead IS NOT NULL THEN l.lead_linear_weight END AS leads_by_lead_date,
        CASE WHEN calculation_type = "mqls_by_lead_date" AND date_mql IS NOT NULL THEN l.lead_linear_weight END AS mqls_by_lead_date,
        CASE WHEN calculation_type = "mqls_by_signup_invoicing_date" AND date_mql IS NOT NULL THEN l.lead_linear_weight END AS mqls_by_signup_invoicing_date,
        CASE WHEN calculation_type = "mqls_by_mql_date" AND date_mql IS NOT NULL THEN l.lead_linear_weight END AS mqls_by_mql_date,
        CASE WHEN calculation_type = "opportunities_by_mql_date" AND date_opportunity IS NOT NULL THEN l.lead_linear_weight END AS opportunities_by_mql_date,
        CASE WHEN calculation_type = "opportunities_by_opportunity_date" AND date_opportunity IS NOT NULL THEN l.lead_linear_weight END AS opportunities_by_opportunity_date,
        CASE WHEN calculation_type = "signups_invoicing_by_signup_invoicing_date" AND date_signup_invoicing IS NOT NULL THEN l.lead_linear_weight END AS signups_invoicing_by_signup_invoicing_date,
        -- CASE WHEN calculation_type = "clients_by_lead_date" AND date_won IS NOT NULL THEN l.lead_linear_weight END AS clients_by_lead_date,
        -- CASE WHEN calculation_type = "clients_by_mql_date" AND date_won IS NOT NULL THEN l.lead_linear_weight END AS clients_by_mql_date,
        -- CASE WHEN calculation_type = "clients_by_won_date" AND date_won IS NOT NULL THEN l.lead_linear_weight END AS clients_by_won_date,
        CASE WHEN calculation_type = "clients_invoicing_by_lead_date" AND date_won_invoicing IS NOT NULL THEN l.lead_linear_weight END AS clients_invoicing_by_lead_date,
        CASE WHEN calculation_type = "clients_invoicing_by_mql_date" AND date_won_invoicing IS NOT NULL THEN l.lead_linear_weight END AS clients_invoicing_by_mql_date,
        CASE WHEN calculation_type = "clients_invoicing_by_won_invoicing_date" AND date_won_invoicing IS NOT NULL THEN l.lead_linear_weight END AS clients_invoicing_by_won_date,
        CASE WHEN calculation_type = "clients_creation_by_lead_date" AND date_won_creation IS NOT NULL THEN l.lead_linear_weight END AS clients_creation_by_lead_date,
        CASE WHEN calculation_type = "clients_creation_by_mql_date" AND date_won_creation IS NOT NULL THEN l.lead_linear_weight END AS clients_creation_by_mql_date,
        CASE WHEN calculation_type = "clients_creation_by_opportunity_date" AND date_won_creation IS NOT NULL THEN l.lead_linear_weight END AS clients_creation_by_opportunity_date,
        CASE WHEN calculation_type = "clients_creation_by_won_creation_date" AND date_won_creation IS NOT NULL THEN l.lead_linear_weight END AS clients_creation_by_won_date,
        CASE WHEN calculation_type = "clients_accounting_by_lead_date" AND date_won_accounting IS NOT NULL THEN l.lead_linear_weight END AS clients_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_accounting_by_mql_date" AND date_won_accounting IS NOT NULL THEN l.lead_linear_weight END AS clients_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL THEN l.lead_linear_weight END AS clients_accounting_by_won_date,
        CASE WHEN calculation_type = "clients_direct_accounting_by_lead_date" AND date_won_accounting IS NOT NULL AND (date_won_invoicing IS NULL OR date_won_invoicing > date_won_accounting) AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN l.lead_linear_weight END AS clients_direct_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_direct_accounting_by_mql_date" AND date_won_accounting IS NOT NULL AND (date_won_invoicing IS NULL OR date_won_invoicing > date_won_accounting) AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN l.lead_linear_weight END AS clients_direct_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_direct_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL AND (date_won_invoicing IS NULL OR date_won_invoicing > date_won_accounting) AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN l.lead_linear_weight END AS clients_direct_accounting_by_won_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_lead_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN l.lead_linear_weight END AS clients_invoicing_to_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_signup_invoicing_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN l.lead_linear_weight END AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_mql_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN l.lead_linear_weight END AS clients_invoicing_to_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN l.lead_linear_weight END AS clients_invoicing_to_accounting_by_won_date,           
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_lead_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN l.lead_linear_weight END AS clients_crea_to_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_mql_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN l.lead_linear_weight END AS clients_crea_to_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_won_creation_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN l.lead_linear_weight END AS clients_crea_to_accounting_by_won_creation_date,                
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN l.lead_linear_weight END AS clients_crea_to_accounting_by_won_date,                
        CASE WHEN calculation_type = "losts_by_lead_date" AND date_lost IS NOT NULL THEN l.lead_linear_weight END AS losts_by_lead_date,
        CASE WHEN calculation_type = "losts_by_lost_date" AND date_lost IS NOT NULL THEN l.lead_linear_weight END AS losts_by_lost_date,
        CASE WHEN calculation_type = "reactivations_by_reactivation_date" AND date_lost < LEAST(date_won_invoicing,date_won_creation,date_won_accounting) THEN l.lead_linear_weight END AS reactivations_by_reactivation_date,
        CAST(NULL AS FLOAT64) AS prestation_amount,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS sessions,
        CAST(NULL AS INT64) AS accountings_goal,
        CAST(NULL AS INT64) AS crea_accounting_transitions_goal,
        CAST(NULL AS INT64) AS invoicing_accounting_transitions_goal,
        CAST(NULL AS INT64) AS direct_accountings_goal,
        CAST(NULL AS INT64) AS creations_goal,
        CAST(NULL AS INT64) AS opportunities_goal,
        CAST(NULL AS INT64) AS mqls_goal,
        CAST(NULL AS INT64) AS leads_goal,
        CAST(NULL AS INT64) AS spend_goal,
        CAST(NULL AS FLOAT64) AS accounting_revenues_goal
    FROM
        {{ ref('int_attributed_contact_revenues') }},UNNEST(lead_linear) l
    CROSS JOIN
        UNNEST(["leads_by_lead_date",
        "mqls_by_lead_date",
        "mqls_by_signup_invoicing_date",
        "mqls_by_mql_date",
        "opportunities_by_mql_date",
        "opportunities_by_opportunity_date",
        "signups_invoicing_by_signup_invoicing_date",
        "clients_invoicing_by_lead_date",
        "clients_invoicing_by_mql_date",
        "clients_invoicing_by_won_invoicing_date",
        "clients_creation_by_lead_date",
        "clients_creation_by_mql_date",
        "clients_creation_by_opportunity_date",
        "clients_creation_by_won_creation_date",
        "clients_accounting_by_lead_date",
        "clients_accounting_by_mql_date",
        "clients_accounting_by_won_accounting_date",
        "clients_direct_accounting_by_lead_date",
        "clients_direct_accounting_by_mql_date",
        "clients_direct_accounting_by_won_accounting_date",
        "clients_invoicing_to_accounting_by_lead_date",
        "clients_invoicing_to_accounting_by_signup_invoicing_date",
        "clients_invoicing_to_accounting_by_mql_date",
        "clients_invoicing_to_accounting_by_won_accounting_date",
        "clients_crea_to_accounting_by_lead_date",
        "clients_crea_to_accounting_by_mql_date",
        "clients_crea_to_accounting_by_won_creation_date",
        "clients_crea_to_accounting_by_won_accounting_date",
        "losts_by_lead_date",
        "losts_by_lost_date",
        "reactivations_by_reactivation_date"]) AS calculation_type
    WHERE
        event_type IS NULL

    UNION ALL

    SELECT
        contact_id,
        dougs_user_id,
        company_id,
        dougs_company_id,
        gender,
        age_range,
        eligible,
        legal_form,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        first_page,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        pre_conversion_touchpoints,
        pre_conversion_forms,
        "mql_linear" AS model_type,
        m.mql_linear_source_name AS source_name,
        m.mql_linear_channel AS channel,
        m.mql_linear_campaign_group AS campaign_group,
        m.mql_linear_utm_term AS term,
        pack_choice,
        CAST(NULL AS STRING) AS prestation_name,
        CAST(NULL AS STRING) AS prestation_category,
        CAST(NULL AS STRING) AS data_prestation_category,
        CASE
            WHEN calculation_type LIKE "%_by_lead_date" THEN date_lead
            WHEN calculation_type LIKE "%_by_mql_date" THEN date_mql
            WHEN calculation_type LIKE "%_by_opportunity_date" THEN date_opportunity
            WHEN calculation_type LIKE "%_by_signup_invoicing_date" THEN date_signup_invoicing
            WHEN calculation_type LIKE "%_by_won_invoicing_date" THEN date_won_invoicing
            WHEN calculation_type LIKE "%_by_won_creation_date" THEN date_won_creation
            WHEN calculation_type LIKE "%_by_won_accounting_date" THEN date_won_accounting
            WHEN calculation_type LIKE "%_by_lost_date" THEN date_lost
            WHEN calculation_type LIKE "%_by_reactivation_date" THEN LEAST(date_won_invoicing,date_won_creation,date_won_accounting)
        END AS date_cohort,
        CASE
            WHEN calculation_type LIKE "leads_by_%" THEN date_lead
            WHEN calculation_type LIKE "mqls_by_%" THEN date_mql
            WHEN calculation_type LIKE "opportunities_by_%" THEN date_opportunity
            WHEN calculation_type LIKE "signups_invoicing_by_%" THEN date_signup_invoicing
            WHEN calculation_type LIKE "clients_invoicing_by_%" THEN date_won_invoicing
            WHEN calculation_type LIKE "clients_creation_by_%" THEN date_won_creation
            WHEN calculation_type LIKE "clients_accounting_by_%" THEN date_won_accounting
            WHEN calculation_type LIKE "clients_direct_accounting_by_%" THEN date_won_accounting
            WHEN calculation_type LIKE "clients_invoicing_to_accounting_by_%" THEN date_won_accounting
            WHEN calculation_type LIKE "clients_crea_to_accounting_by_%" THEN date_won_invoicing
            WHEN calculation_type LIKE "losts_by_%" THEN date_lost
            WHEN calculation_type LIKE "reactivations_by_%" THEN LEAST(date_won_invoicing,date_won_creation,date_won_accounting)
        END AS date_actual,
        CASE WHEN calculation_type = "leads_by_lead_date" AND date_lead IS NOT NULL THEN m.mql_linear_weight END AS leads_by_lead_date,
        CASE WHEN calculation_type = "mqls_by_lead_date" AND date_mql IS NOT NULL THEN m.mql_linear_weight END AS mqls_by_lead_date,
        CASE WHEN calculation_type = "mqls_by_signup_invoicing_date" AND date_mql IS NOT NULL THEN m.mql_linear_weight END AS mqls_by_signup_invoicing_date,
        CASE WHEN calculation_type = "mqls_by_mql_date" AND date_mql IS NOT NULL THEN m.mql_linear_weight END AS mqls_by_mql_date,
        CASE WHEN calculation_type = "opportunities_by_mql_date" AND date_opportunity IS NOT NULL THEN m.mql_linear_weight END AS opportunities_by_mql_date,
        CASE WHEN calculation_type = "opportunities_by_opportunity_date" AND date_opportunity IS NOT NULL THEN m.mql_linear_weight END AS opportunities_by_opportunity_date,
        CASE WHEN calculation_type = "signups_invoicing_by_signup_invoicing_date" AND date_signup_invoicing IS NOT NULL THEN m.mql_linear_weight END AS signups_invoicing_by_signup_invoicing_date,
        -- CASE WHEN calculation_type = "clients_by_lead_date" AND date_won IS NOT NULL THEN m.mql_linear_weight END AS clients_by_lead_date,
        -- CASE WHEN calculation_type = "clients_by_mql_date" AND date_won IS NOT NULL THEN m.mql_linear_weight END AS clients_by_mql_date,
        -- CASE WHEN calculation_type = "clients_by_won_date" AND date_won IS NOT NULL THEN m.mql_linear_weight END AS clients_by_won_date,
        CASE WHEN calculation_type = "clients_invoicing_by_lead_date" AND date_won_invoicing IS NOT NULL THEN m.mql_linear_weight END AS clients_invoicing_by_lead_date,
        CASE WHEN calculation_type = "clients_invoicing_by_mql_date" AND date_won_invoicing IS NOT NULL THEN m.mql_linear_weight END AS clients_invoicing_by_mql_date,
        CASE WHEN calculation_type = "clients_invoicing_by_won_invoicing_date" AND date_won_invoicing IS NOT NULL THEN m.mql_linear_weight END AS clients_invoicing_by_won_date,
        CASE WHEN calculation_type = "clients_creation_by_lead_date" AND date_won_creation IS NOT NULL THEN m.mql_linear_weight END AS clients_creation_by_lead_date,
        CASE WHEN calculation_type = "clients_creation_by_mql_date" AND date_won_creation IS NOT NULL THEN m.mql_linear_weight END AS clients_creation_by_mql_date,
        CASE WHEN calculation_type = "clients_creation_by_opportunity_date" AND date_won_creation IS NOT NULL THEN m.mql_linear_weight END AS clients_creation_by_opportunity_date,
        CASE WHEN calculation_type = "clients_creation_by_won_creation_date" AND date_won_creation IS NOT NULL THEN m.mql_linear_weight END AS clients_creation_by_won_date,
        CASE WHEN calculation_type = "clients_accounting_by_lead_date" AND date_won_accounting IS NOT NULL THEN m.mql_linear_weight END AS clients_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_accounting_by_mql_date" AND date_won_accounting IS NOT NULL THEN m.mql_linear_weight END AS clients_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL THEN m.mql_linear_weight END AS clients_accounting_by_won_date,
        CASE WHEN calculation_type = "clients_direct_accounting_by_lead_date" AND date_won_accounting IS NOT NULL AND (date_won_invoicing IS NULL OR date_won_invoicing > date_won_accounting) AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN m.mql_linear_weight END AS clients_direct_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_direct_accounting_by_mql_date" AND date_won_accounting IS NOT NULL AND (date_won_invoicing IS NULL OR date_won_invoicing > date_won_accounting) AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN m.mql_linear_weight END AS clients_direct_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_direct_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL AND (date_won_invoicing IS NULL OR date_won_invoicing > date_won_accounting) AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN m.mql_linear_weight END AS clients_direct_accounting_by_won_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_lead_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN m.mql_linear_weight END AS clients_invoicing_to_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_signup_invoicing_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN m.mql_linear_weight END AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_mql_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN m.mql_linear_weight END AS clients_invoicing_to_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_invoicing_to_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL AND date_won_invoicing <= date_won_accounting AND (date_won_creation IS NULL OR date_won_creation > date_won_accounting) THEN m.mql_linear_weight END AS clients_invoicing_to_accounting_by_won_date,           
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_lead_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN m.mql_linear_weight END AS clients_crea_to_accounting_by_lead_date,
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_mql_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN m.mql_linear_weight END AS clients_crea_to_accounting_by_mql_date,
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_won_creation_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN m.mql_linear_weight END AS clients_crea_to_accounting_by_won_creation_date,                
        CASE WHEN calculation_type = "clients_crea_to_accounting_by_won_accounting_date" AND date_won_accounting IS NOT NULL AND date_won_creation <= date_won_accounting THEN m.mql_linear_weight END AS clients_crea_to_accounting_by_won_date,                
        CASE WHEN calculation_type = "losts_by_lead_date" AND date_lost IS NOT NULL THEN m.mql_linear_weight END AS losts_by_lead_date,
        CASE WHEN calculation_type = "losts_by_lost_date" AND date_lost IS NOT NULL THEN m.mql_linear_weight END AS losts_by_lost_date,
        CASE WHEN calculation_type = "reactivations_by_reactivation_date" AND date_lost < LEAST(date_won_invoicing,date_won_creation,date_won_accounting) THEN m.mql_linear_weight END AS reactivations_by_reactivation_date,
        CAST(NULL AS FLOAT64) AS prestation_amount,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS sessions,
        CAST(NULL AS INT64) AS accountings_goal,
        CAST(NULL AS INT64) AS crea_accounting_transitions_goal,
        CAST(NULL AS INT64) AS invoicing_accounting_transitions_goal,
        CAST(NULL AS INT64) AS direct_accountings_goal,
        CAST(NULL AS INT64) AS creations_goal,
        CAST(NULL AS INT64) AS opportunities_goal,
        CAST(NULL AS INT64) AS mqls_goal,
        CAST(NULL AS INT64) AS leads_goal,
        CAST(NULL AS INT64) AS spend_goal,
        CAST(NULL AS FLOAT64) AS accounting_revenues_goal
    FROM
        {{ ref('int_attributed_contact_revenues') }},UNNEST(mql_linear) m
    CROSS JOIN
        UNNEST(["leads_by_lead_date",
        "mqls_by_lead_date",
        "mqls_by_signup_invoicing_date",
        "mqls_by_mql_date",
        "opportunities_by_mql_date",
        "opportunities_by_opportunity_date",
        "signups_invoicing_by_signup_invoicing_date",
        "clients_invoicing_by_lead_date",
        "clients_invoicing_by_mql_date",
        "clients_invoicing_by_won_invoicing_date",
        "clients_creation_by_lead_date",
        "clients_creation_by_mql_date",
        "clients_creation_by_opportunity_date",
        "clients_creation_by_won_creation_date",
        "clients_accounting_by_lead_date",
        "clients_accounting_by_mql_date",
        "clients_accounting_by_won_accounting_date",
        "clients_direct_accounting_by_lead_date",
        "clients_direct_accounting_by_mql_date",
        "clients_direct_accounting_by_won_accounting_date",
        "clients_invoicing_to_accounting_by_lead_date",
        "clients_invoicing_to_accounting_by_signup_invoicing_date",
        "clients_invoicing_to_accounting_by_mql_date",
        "clients_invoicing_to_accounting_by_won_accounting_date",
        "clients_crea_to_accounting_by_lead_date",
        "clients_crea_to_accounting_by_mql_date",
        "clients_crea_to_accounting_by_won_creation_date",
        "clients_crea_to_accounting_by_won_accounting_date",
        "losts_by_lead_date",
        "losts_by_lost_date",
        "reactivations_by_reactivation_date"]) AS calculation_type
    WHERE
        event_type IS NULL        
        
    UNION ALL

    SELECT
        contact_id,
        dougs_user_id,
        company_id,
        dougs_company_id,
        gender,
        age_range,
        eligible,
        legal_form,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        first_page,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        CAST(NULL AS INT64) AS pre_conversion_touchpoints,
        CAST(NULL AS INT64) AS pre_conversion_forms,
        model_type,
        CASE
            WHEN model_type = "first_touch" THEN first_touchpoint_source_name
            WHEN model_type = "first_form" THEN first_form_source_name
            WHEN model_type = "lead_last_touch" THEN lead_last_touchpoint_source_name
            WHEN model_type = "mql_last_touch" THEN mql_last_touchpoint_source_name
        END AS source_name,
        CASE
            WHEN model_type = "first_touch" THEN first_touchpoint_channel
            WHEN model_type = "first_form" THEN first_form_channel
            WHEN model_type = "lead_last_touch" THEN lead_last_touchpoint_channel
            WHEN model_type = "mql_last_touch" THEN mql_last_touchpoint_channel
        END AS channel,
        CASE
            WHEN model_type = "first_touch" THEN first_touchpoint_campaign_group
            WHEN model_type = "first_form" THEN first_form_campaign_group
            WHEN model_type = "lead_last_touch" THEN lead_last_touchpoint_campaign_group
            WHEN model_type = "mql_last_touch" THEN mql_last_touchpoint_campaign_group
        END AS campaign_group, 
        CASE
            WHEN model_type = "first_touch" THEN first_touchpoint_term
            WHEN model_type = "first_form" THEN first_form_term
            WHEN model_type = "lead_last_touch" THEN lead_last_touchpoint_term
            WHEN model_type = "mql_last_touch" THEN mql_last_touchpoint_term
        END AS term,       
        pack_choice,
        prestation_name,
        prestation_category,
        data_prestation_category,
        event_date AS date_cohort,
        event_date AS date_actual,
        CAST(NULL AS INT64) AS leads_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS mqls_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_opportunity_date,
        CAST(NULL AS INT64) AS signups_invoicing_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_won_date,
        CAST(NULL AS INT64) AS clients_creation_by_lead_date,
        CAST(NULL AS INT64) AS clients_creation_by_mql_date,
        CAST(NULL AS INT64) AS clients_creation_by_opportunity_date,
        CAST(NULL AS INT64) AS clients_creation_by_won_date,
        CAST(NULL AS INT64) AS clients_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_creation_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS losts_by_lead_date,
        CAST(NULL AS INT64) AS losts_by_lost_date,
        CAST(NULL AS INT64) AS reactivations_by_reactivation_date,
        ROUND(prestation_amount,2) AS prestation_amount,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS sessions,
        CAST(NULL AS INT64) AS accountings_goal,
        CAST(NULL AS INT64) AS crea_accounting_transitions_goal,
        CAST(NULL AS INT64) AS invoicing_accounting_transitions_goal,
        CAST(NULL AS INT64) AS direct_accountings_goal,
        CAST(NULL AS INT64) AS creations_goal,
        CAST(NULL AS INT64) AS opportunities_goal,
        CAST(NULL AS INT64) AS mqls_goal,
        CAST(NULL AS INT64) AS leads_goal,
        CAST(NULL AS INT64) AS spend_goal,
        CAST(NULL AS FLOAT64) AS accounting_revenues_goal              
    FROM
        {{ ref('int_attributed_contact_revenues') }}
    CROSS JOIN
        UNNEST(["first_touch",
        "first_form",
        "lead_last_touch",
        "mql_last_touch"
        ]) AS model_type         
    WHERE
        event_type = 'prestation'

    UNION ALL

    SELECT
        contact_id,
        dougs_user_id,
        company_id,
        dougs_company_id,
        gender,
        age_range,
        eligible,
        legal_form,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        first_page,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        CAST(NULL AS INT64) AS pre_conversion_touchpoints,
        CAST(NULL AS INT64) AS pre_conversion_forms,
        "lead_linear" AS model_type,
        l.lead_linear_source_name AS source_name,
        l.lead_linear_channel AS channel,
        l.lead_linear_campaign_group AS campaign_group,
        l.lead_linear_utm_term AS term,     
        pack_choice,
        prestation_name,
        prestation_category,
        data_prestation_category,
        event_date AS date_cohort,
        event_date AS date_actual,
        CAST(NULL AS INT64) AS leads_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS mqls_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_opportunity_date,
        CAST(NULL AS INT64) AS signups_invoicing_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_won_date,
        CAST(NULL AS INT64) AS clients_creation_by_lead_date,
        CAST(NULL AS INT64) AS clients_creation_by_mql_date,
        CAST(NULL AS INT64) AS clients_creation_by_opportunity_date,
        CAST(NULL AS INT64) AS clients_creation_by_won_date,
        CAST(NULL AS INT64) AS clients_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_creation_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS losts_by_lead_date,
        CAST(NULL AS INT64) AS losts_by_lost_date,
        CAST(NULL AS INT64) AS reactivations_by_reactivation_date,
        ROUND(prestation_amount,2) AS prestation_amount,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS sessions,
        CAST(NULL AS INT64) AS accountings_goal,
        CAST(NULL AS INT64) AS crea_accounting_transitions_goal,
        CAST(NULL AS INT64) AS invoicing_accounting_transitions_goal,
        CAST(NULL AS INT64) AS direct_accountings_goal,
        CAST(NULL AS INT64) AS creations_goal,
        CAST(NULL AS INT64) AS opportunities_goal,
        CAST(NULL AS INT64) AS mqls_goal,
        CAST(NULL AS INT64) AS leads_goal,
        CAST(NULL AS INT64) AS spend_goal,
        CAST(NULL AS FLOAT64) AS accounting_revenues_goal              
    FROM
        {{ ref('int_attributed_contact_revenues') }},UNNEST(lead_linear) l      
    WHERE
        event_type = 'prestation'

    UNION ALL

    SELECT
        contact_id,
        dougs_user_id,
        company_id,
        dougs_company_id,
        gender,
        age_range,
        eligible,
        legal_form,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        first_page,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        CAST(NULL AS INT64) AS pre_conversion_touchpoints,
        CAST(NULL AS INT64) AS pre_conversion_forms,
        "mql_linear" AS model_type,
        m.mql_linear_source_name AS source_name,
        m.mql_linear_channel AS channel,
        m.mql_linear_campaign_group AS campaign_group,
        m.mql_linear_utm_term AS term,     
        pack_choice,
        prestation_name,
        prestation_category,
        data_prestation_category,
        event_date AS date_cohort,
        event_date AS date_actual,
        CAST(NULL AS INT64) AS leads_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS mqls_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_opportunity_date,
        CAST(NULL AS INT64) AS signups_invoicing_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_won_date,
        CAST(NULL AS INT64) AS clients_creation_by_lead_date,
        CAST(NULL AS INT64) AS clients_creation_by_mql_date,
        CAST(NULL AS INT64) AS clients_creation_by_opportunity_date,
        CAST(NULL AS INT64) AS clients_creation_by_won_date,
        CAST(NULL AS INT64) AS clients_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_creation_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS losts_by_lead_date,
        CAST(NULL AS INT64) AS losts_by_lost_date,
        CAST(NULL AS INT64) AS reactivations_by_reactivation_date,
        ROUND(prestation_amount,2) AS prestation_amount,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS sessions,
        CAST(NULL AS INT64) AS accountings_goal,
        CAST(NULL AS INT64) AS crea_accounting_transitions_goal,
        CAST(NULL AS INT64) AS invoicing_accounting_transitions_goal,
        CAST(NULL AS INT64) AS direct_accountings_goal,
        CAST(NULL AS INT64) AS creations_goal,
        CAST(NULL AS INT64) AS opportunities_goal,
        CAST(NULL AS INT64) AS mqls_goal,
        CAST(NULL AS INT64) AS leads_goal,
        CAST(NULL AS INT64) AS spend_goal,
        CAST(NULL AS FLOAT64) AS accounting_revenues_goal              
    FROM
        {{ ref('int_attributed_contact_revenues') }},UNNEST(mql_linear) m      
    WHERE
        event_type = 'prestation'             
  
    UNION ALL

    SELECT
        CAST(NULL AS STRING) AS contact_id,
        CAST(NULL AS STRING) AS dougs_user_id,
        CAST(NULL AS STRING) AS company_id,
        CAST(NULL AS STRING) AS dougs_company_id,
        CAST(NULL AS STRING) AS gender,
        CAST(NULL AS STRING) AS age_range,
        CAST(NULL AS BOOL) AS eligible,
        CAST(NULL AS STRING) AS legal_form,
        CAST(NULL AS STRING) AS ape_activity_name,
        CAST(NULL AS STRING) AS activity,
        CAST(NULL AS STRING) AS first_conversion_form,
        CAST(NULL AS STRING) AS first_conversion_form_category,
        CAST(NULL AS STRING) AS first_conversion_form_type,
        CAST(NULL AS STRING) AS first_page,
        CAST(NULL AS STRING) AS contact_category,
        CAST(NULL AS STRING) AS lost_reason,
        CAST(NULL AS STRING) AS contact_type,
        CAST(NULL AS STRING) AS treatment_type,
        CAST(NULL AS INT64) AS pre_conversion_touchpoints,
        CAST(NULL AS INT64) AS pre_conversion_forms,
        CAST(NULL AS STRING) AS model_type,
        source_name,
        channel,
        IFNULL(FIRST_VALUE(c.campaign_group) OVER w,"Unmapped") AS campaign_group,
        keyword_name AS term,         
        CAST(NULL AS STRING) AS pack_choice,
        CAST(NULL AS STRING) AS prestation_name,
        CAST(NULL AS STRING) AS prestation_category,
        CAST(NULL AS STRING) AS data_prestation_category,                
        date_report AS date_cohort,
        date_report AS date_actual,
        CAST(NULL AS INT64) AS leads_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS mqls_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_opportunity_date,
        CAST(NULL AS INT64) AS signups_invoicing_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_won_date,
        CAST(NULL AS INT64) AS clients_creation_by_lead_date,
        CAST(NULL AS INT64) AS clients_creation_by_mql_date,
        CAST(NULL AS INT64) AS clients_creation_by_opportunity_date,
        CAST(NULL AS INT64) AS clients_creation_by_won_date,
        CAST(NULL AS INT64) AS clients_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_creation_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS losts_by_lead_date,
        CAST(NULL AS INT64) AS losts_by_lost_date,
        CAST(NULL AS INT64) AS reactivations_by_reactivation_date,        
        CAST(NULL AS FLOAT64) AS prestation_amount,
        impressions,
        clicks,
        spend,
        CAST(NULL AS INT64) AS sessions,
        CAST(NULL AS INT64) AS accountings_goal,
        CAST(NULL AS INT64) AS crea_accounting_transitions_goal,
        CAST(NULL AS INT64) AS invoicing_accounting_transitions_goal,
        CAST(NULL AS INT64) AS direct_accountings_goal,
        CAST(NULL AS INT64) AS creations_goal,
        CAST(NULL AS INT64) AS opportunities_goal,
        CAST(NULL AS INT64) AS mqls_goal,
        CAST(NULL AS INT64) AS leads_goal,
        CAST(NULL AS INT64) AS spend_goal,
        CAST(NULL AS FLOAT64) AS accounting_revenues_goal             
    FROM
        {{ ref('int_advertising_statistics') }} a
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} c
    ON
        (c.utm_campaign IS NULL OR LOWER(a.campaign_name) LIKE LOWER(c.utm_campaign))            
    WINDOW w AS (
        PARTITION BY c.utm_campaign, c.utm_source, c.utm_medium
        ORDER BY c.sort ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )           
  
    UNION ALL

    SELECT
        CAST(NULL AS STRING) contact_id,
        CAST(NULL AS STRING) dougs_user_id,
        CAST(NULL AS STRING) company_id,
        CAST(NULL AS STRING) dougs_company_id,
        CAST(NULL AS STRING) AS gender,
        CAST(NULL AS STRING) AS age_range,
        CAST(NULL AS BOOL) AS eligible,
        CAST(NULL AS STRING) AS legal_form,
        CAST(NULL AS STRING) AS ape_activity_name,
        CAST(NULL AS STRING) AS activity,
        CAST(NULL AS STRING) AS first_conversion_form,
        CAST(NULL AS STRING) AS first_conversion_form_category,
        CAST(NULL AS STRING) AS first_conversion_form_type,
        first_page,
        CAST(NULL AS STRING) AS contact_category,
        CAST(NULL AS STRING) AS lost_reason,
        CAST(NULL AS STRING) AS contact_type,
        CAST(NULL AS STRING) AS treatment_type,
        CAST(NULL AS INT64) AS pre_conversion_touchpoints,
        CAST(NULL AS INT64) AS pre_conversion_forms,
        CAST(NULL AS STRING) AS model_type,
        session_source_name AS source_name,
        session_channel AS channel,
        IFNULL(FIRST_VALUE(c.campaign_group) OVER w,"Unmapped") AS campaign_group,
        utm_term AS term,
        CAST(NULL AS STRING) AS pack_choice,
        CAST(NULL AS STRING) AS prestation_name,    
        CAST(NULL AS STRING) AS prestation_category,
        CAST(NULL AS STRING) AS data_prestation_category,            
        date_report AS date_cohort,
        date_report AS date_actual,
        CAST(NULL AS INT64) AS leads_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS mqls_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_opportunity_date,
        CAST(NULL AS INT64) AS signups_invoicing_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_won_date,
        CAST(NULL AS INT64) AS clients_creation_by_lead_date,
        CAST(NULL AS INT64) AS clients_creation_by_mql_date,
        CAST(NULL AS INT64) AS clients_creation_by_opportunity_date,
        CAST(NULL AS INT64) AS clients_creation_by_won_date,
        CAST(NULL AS INT64) AS clients_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_creation_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS losts_by_lead_date,
        CAST(NULL AS INT64) AS losts_by_lost_date,
        CAST(NULL AS INT64) AS reactivations_by_reactivation_date,
        CAST(NULL AS FLOAT64) AS prestation_amount,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(NULL AS FLOAT64) AS spend,
        COUNT(DISTINCT session_id) AS sessions,
        CAST(NULL AS INT64) AS accountings_goal,
        CAST(NULL AS INT64) AS crea_accounting_transitions_goal,
        CAST(NULL AS INT64) AS invoicing_accounting_transitions_goal,
        CAST(NULL AS INT64) AS direct_accountings_goal,
        CAST(NULL AS INT64) AS creations_goal,
        CAST(NULL AS INT64) AS opportunities_goal,
        CAST(NULL AS INT64) AS mqls_goal,
        CAST(NULL AS INT64) AS leads_goal,
        CAST(NULL AS INT64) AS spend_goal,
        CAST(NULL AS FLOAT64) AS accounting_revenues_goal      
    FROM
        {{ ref('int_google_analytics_4') }} g
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} c
    ON
        (c.utm_campaign IS NULL OR LOWER(g.utm_campaign) LIKE LOWER(c.utm_campaign))
        AND
        (c.utm_source IS NULL OR LOWER(g.utm_source) LIKE LOWER(c.utm_source))
        AND
        (c.utm_medium IS NULL OR LOWER(g.utm_medium) LIKE LOWER(c.utm_medium))
    GROUP BY
        first_page,
        source_name,
        channel,
        c.campaign_group,
        c.utm_campaign,
        c.utm_source,
        c.utm_medium,
        c.sort,
        term,        
        date_cohort,
        date_actual        
    WINDOW w AS (
        PARTITION BY c.utm_campaign, c.utm_source, c.utm_medium
        ORDER BY c.sort ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )

    UNION ALL

    SELECT
        CAST(NULL AS STRING) contact_id,
        CAST(NULL AS STRING) dougs_user_id,
        CAST(NULL AS STRING) company_id,
        CAST(NULL AS STRING) dougs_company_id,
        CAST(NULL AS STRING) AS gender,
        CAST(NULL AS STRING) AS age_range,
        CAST(NULL AS BOOL) AS eligible,
        CAST(NULL AS STRING) AS legal_form,
        CAST(NULL AS STRING) AS ape_activity_name,
        CAST(NULL AS STRING) AS activity,
        CAST(NULL AS STRING) AS first_conversion_form,
        CAST(NULL AS STRING) AS first_conversion_form_category,
        CAST(NULL AS STRING) AS first_conversion_form_type,
        CAST(NULL AS STRING) AS first_page,
        CAST(NULL AS STRING) AS contact_category,
        CAST(NULL AS STRING) AS lost_reason,
        CAST(NULL AS STRING) AS contact_type,
        CAST(NULL AS STRING) AS treatment_type,
        CAST(NULL AS INT64) AS pre_conversion_touchpoints,
        CAST(NULL AS INT64) AS pre_conversion_forms,   
        CAST(NULL AS STRING) AS model_type,
        CAST(NULL AS STRING) AS source_name,
        CAST(NULL AS STRING) AS channel,
        CAST(NULL AS STRING) AS campaign_group,
        CAST(NULL AS STRING) AS term,             
        CAST(NULL AS STRING) AS pack_choice,
        CAST(NULL AS STRING) AS prestation_name,    
        CAST(NULL AS STRING) AS prestation_category,
        CAST(NULL AS STRING) AS data_prestation_category,            
        date AS date_cohort,
        date AS date_actual,
        CAST(NULL AS INT64) AS leads_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_lead_date,
        CAST(NULL AS INT64) AS mqls_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS mqls_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_mql_date,
        CAST(NULL AS INT64) AS opportunities_by_opportunity_date,
        CAST(NULL AS INT64) AS signups_invoicing_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_by_won_date,
        CAST(NULL AS INT64) AS clients_creation_by_lead_date,
        CAST(NULL AS INT64) AS clients_creation_by_mql_date,
        CAST(NULL AS INT64) AS clients_creation_by_opportunity_date,
        CAST(NULL AS INT64) AS clients_creation_by_won_date,
        CAST(NULL AS INT64) AS clients_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_direct_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_invoicing_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_lead_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_mql_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_creation_date,
        CAST(NULL AS INT64) AS clients_crea_to_accounting_by_won_date,
        CAST(NULL AS INT64) AS losts_by_lead_date,
        CAST(NULL AS INT64) AS losts_by_lost_date,
        CAST(NULL AS INT64) AS reactivations_by_reactivation_date,
        CAST(NULL AS FLOAT64) AS prestation_amount,
        CAST(NULL AS INT64) AS impressions,
        CAST(NULL AS INT64) AS clicks,
        CAST(NULL AS FLOAT64) AS spend,
        CAST(NULL AS INT64) AS sessions,
        accountings_goal,
        crea_accounting_transitions_goal,
        invoicing_accounting_transitions_goal,
        direct_accountings_goal,
        creations_goal,
        opportunities_goal,
        mqls_goal,
        leads_goal,
        spend_goal,
        accounting_revenues_goal
    FROM
        daily_goals
    GROUP BY
        date_cohort,
        date_actual,
        accountings_goal,
        crea_accounting_transitions_goal,
        invoicing_accounting_transitions_goal,
        direct_accountings_goal,
        creations_goal,
        opportunities_goal,
        mqls_goal,
        leads_goal,
        spend_goal,
        accounting_revenues_goal      
),
indicators_on_main_breakdowns AS (
    SELECT
        date_cohort,
        date_actual,
        gender,
        age_range,
        eligible,
        legal_form,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        first_page,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        pack_choice,
        prestation_name,    
        prestation_category,
        data_prestation_category,
        model_type,
        source_name,
        channel,
        campaign_group,
        term,
        AVG(pre_conversion_touchpoints) AS pre_conversion_touchpoints,
        AVG(pre_conversion_forms) AS pre_conversion_forms,
        SUM(leads_by_lead_date) AS leads_by_lead_date,
        SUM(mqls_by_lead_date) AS mqls_by_lead_date,
        SUM(mqls_by_signup_invoicing_date) AS mqls_by_signup_invoicing_date,
        SUM(mqls_by_mql_date) AS mqls_by_mql_date,
        SUM(opportunities_by_mql_date) AS opportunities_by_mql_date,
        SUM(opportunities_by_opportunity_date) AS opportunities_by_opportunity_date,
        SUM(signups_invoicing_by_signup_invoicing_date) AS signups_invoicing_by_signup_invoicing_date,
        SUM(clients_invoicing_by_lead_date) AS clients_invoicing_by_lead_date,
        SUM(clients_invoicing_by_mql_date) AS clients_invoicing_by_mql_date,
        SUM(clients_invoicing_by_won_date) AS clients_invoicing_by_won_date,
        SUM(clients_creation_by_lead_date) AS clients_creation_by_lead_date,
        SUM(clients_creation_by_mql_date) AS clients_creation_by_mql_date,
        SUM(clients_creation_by_opportunity_date) AS clients_creation_by_opportunity_date,
        SUM(clients_creation_by_won_date) AS clients_creation_by_won_date,
        SUM(clients_accounting_by_lead_date) AS clients_accounting_by_lead_date,
        SUM(clients_accounting_by_mql_date) AS clients_accounting_by_mql_date,
        SUM(clients_accounting_by_won_date) AS clients_accounting_by_won_date,
        SUM(clients_direct_accounting_by_lead_date) AS clients_direct_accounting_by_lead_date,
        SUM(clients_direct_accounting_by_mql_date) AS clients_direct_accounting_by_mql_date,
        SUM(clients_direct_accounting_by_won_date) AS clients_direct_accounting_by_won_date,
        SUM(clients_invoicing_to_accounting_by_lead_date) AS clients_invoicing_to_accounting_by_lead_date,
        SUM(clients_invoicing_to_accounting_by_signup_invoicing_date) AS clients_invoicing_to_accounting_by_signup_invoicing_date,
        SUM(clients_invoicing_to_accounting_by_mql_date) AS clients_invoicing_to_accounting_by_mql_date,
        SUM(clients_invoicing_to_accounting_by_won_date) AS clients_invoicing_to_accounting_by_won_date,
        SUM(clients_crea_to_accounting_by_lead_date) AS clients_crea_to_accounting_by_lead_date,
        SUM(clients_crea_to_accounting_by_mql_date) AS clients_crea_to_accounting_by_mql_date,
        SUM(clients_crea_to_accounting_by_won_creation_date) AS clients_crea_to_accounting_by_won_creation_date,
        SUM(clients_crea_to_accounting_by_won_date) AS clients_crea_to_accounting_by_won_date,
        SUM(losts_by_lead_date) AS losts_by_lead_date,
        SUM(losts_by_lost_date) AS losts_by_lost_date,
        SUM(reactivations_by_reactivation_date) AS reactivations_by_reactivation_date,
        SUM(prestation_amount) AS prestation_amount,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(spend) AS spend,
        SUM(sessions) AS sessions,
        SUM(accountings_goal) AS accountings_goal,
        SUM(crea_accounting_transitions_goal) AS crea_accounting_transitions_goal,
        SUM(invoicing_accounting_transitions_goal) AS invoicing_accounting_transitions_goal,
        SUM(direct_accountings_goal) AS direct_accountings_goal,
        SUM(creations_goal) AS creations_goal,
        SUM(opportunities_goal) AS opportunities_goal,
        SUM(mqls_goal) AS mqls_goal,
        SUM(leads_goal) AS leads_goal,
        SUM(spend_goal) AS spend_goal,
        SUM(accounting_revenues_goal) AS accounting_revenues_goal
    FROM
        indicators_from_all_sources
    GROUP BY
        date_cohort,
        date_actual,
        gender,
        age_range,
        eligible,
        legal_form,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        first_page,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        pack_choice,
        prestation_name,
        prestation_category,
        data_prestation_category,
        model_type,
        source_name,
        channel,
        campaign_group,
        term               
)
SELECT * FROM indicators_on_main_breakdowns WHERE date_cohort >= "2023-10-01"
