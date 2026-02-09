WITH form_submissions AS (
    SELECT
        form_id,
        form_submission_id,
        contact_id,
        submission_timestamp,
        page_url,
        email_address,
        product_type,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_id,
        utm_content,
        utm_term,
        referrer,
        landing_page,
        gclid,
        fbclid,
        partner_referral_id,
        ga_visitor_id,
    FROM
        {{ ref('fct_hubspot_form_submissions') }}
    WHERE
        contact_id IS NOT NULL
),

hs_contacts_ga_visitors_gathered AS (
    SELECT DISTINCT
        contact_id,
        ga_visitor_id,
    FROM
        form_submissions
    
    UNION ALL

    SELECT DISTINCT
        hs_contact_id AS contact_id,
        session_ga_visitor_id AS ga_visitor_id,
    FROM
        {{ ref('stg_airbyte_hubspot_event_calendly_email_provided') }}
    
    UNION ALL

    SELECT DISTINCT
        hs_contact_id AS contact_id,
        session_ga_visitor_id AS ga_visitor_id,
    FROM
        {{ ref('stg_airbyte_hubspot_event_intercom_email_provided') }}
    
    UNION ALL

    SELECT DISTINCT
        hs_contact_id AS contact_id,
        session_ga_visitor_id AS ga_visitor_id,
    FROM
        {{ ref('stg_airbyte_hubspot_event_signup_web_app') }}
),

hs_contacts_ga_visitors_unique AS (
    SELECT DISTINCT
        contact_id,
        ga_visitor_id,
    FROM
        hs_contacts_ga_visitors_gathered
    WHERE
        contact_id IS NOT NULL
        AND ga_visitor_id IS NOT NULL
),

hs_contacts_first_pages AS (
    SELECT
        hs_contact_id AS contact_id,
        REGEXP_EXTRACT(url, r"^(?:https?://)?[^/]+(/[^?#]*)") AS contact_first_page
    FROM
        {{ ref('stg_airbyte_hubspot_event_visited_page') }} AS p
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY hs_contact_id
            ORDER BY event_timestamp ASC
        ) = 1        
),

all_touchpoints AS (
    SELECT
        contact_id,
        "Hubspot Form Submission" AS touchpoint_type,
        CONCAT("HFS-", form_submission_id) AS touchpoint_id,
        submission_timestamp AS touchpoint_timestamp,
        "form_submission" AS web_event_type,
        CASE
            WHEN form_id IN ('f27af304-1f3c-45fc-a15b-038cb78cca52',
    'cd0a1662-864c-4be2-910b-09b879a9ba42') THEN 'standard_tel'
            ELSE utm_source
        END AS utm_source,
        CASE
            WHEN form_id IN ('f27af304-1f3c-45fc-a15b-038cb78cca52',
    'cd0a1662-864c-4be2-910b-09b879a9ba42') THEN 'sales'
            ELSE utm_medium
        END AS utm_medium,
        CASE
            WHEN form_id IN ('f27af304-1f3c-45fc-a15b-038cb78cca52',
    'cd0a1662-864c-4be2-910b-09b879a9ba42') THEN 'saisie_sales'
            ELSE utm_campaign
        END AS utm_campaign,
        utm_id,
        utm_content,
        utm_term,
        referrer,
        landing_page,
        gclid,
        fbclid,
        partner_referral_id,
    FROM
        form_submissions

    UNION ALL

    SELECT
        contact_id,
        "Hubspot Session" AS touchpoint_type,
        CONCAT("HSE-", session_id) AS touchpoint_id,
        session_start_timestamp AS touchpoint_timestamp,
        "session_start" AS web_event_type,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_id,
        utm_content,
        utm_term,
        referrer,
        landing_page,
        gclid,
        fbclid,
        partner_referral_id,
    FROM
        {{ ref('fct_hubspot_sessions') }}

    UNION ALL

    SELECT
        hs_contact_id AS contact_id,
        "Calendly Email" AS touchpoint_type,
        CONCAT("CAL-", hs_event_id) AS touchpoint_id,
        event_timestamp AS touchpoint_timestamp,
        web_event_type,        
        session_utm_source AS utm_source,
        session_utm_medium AS utm_medium,
        session_utm_campaign AS utm_campaign,
        session_utm_id AS utm_id,
        session_utm_content AS utm_content,
        session_utm_term AS utm_term,
        session_referrer AS referrer,
        session_landing_page AS landing_page,
        session_gclid AS gclid,
        session_fbclid AS fbclid,
        REGEXP_EXTRACT(session_landing_page, r"[\?&]r=([^&]*)") AS partner_referral_id,
    FROM
        {{ ref('stg_airbyte_hubspot_event_calendly_email_provided') }}

    UNION ALL

    SELECT
        hs_contact_id AS contact_id,
        "Intercom Email" AS touchpoint_type,
        CONCAT("INT-", hs_event_id) AS touchpoint_id,
        event_timestamp AS touchpoint_timestamp,
        web_event_type,
        session_utm_source AS utm_source,
        session_utm_medium AS utm_medium,
        session_utm_campaign AS utm_campaign,
        session_utm_id AS utm_id,
        session_utm_content AS utm_content,
        session_utm_term AS utm_term,
        session_referrer AS referrer,
        session_landing_page AS landing_page,
        session_gclid AS gclid,
        session_fbclid AS fbclid,
        REGEXP_EXTRACT(session_landing_page, r"[\?&]r=([^&]*)") AS partner_referral_id,
    FROM
        {{ ref('stg_airbyte_hubspot_event_intercom_email_provided') }}

    UNION ALL

    SELECT
        hs_contact_id AS contact_id,
        "Sign Up" AS touchpoint_type,
        CONCAT("SIU-", hs_event_id) AS touchpoint_id,
        event_timestamp AS touchpoint_timestamp,
        web_event_type,
        session_utm_source AS utm_source,
        session_utm_medium AS utm_medium,
        session_utm_campaign AS utm_campaign,
        session_utm_id AS utm_id,
        session_utm_content AS utm_content,
        session_utm_term AS utm_term,
        session_referrer AS referrer,
        session_landing_page AS landing_page,
        session_gclid AS gclid,
        session_fbclid AS fbclid,
        REGEXP_EXTRACT(session_landing_page, r"[\?&]r=([^&]*)") AS partner_referral_id,
    FROM
        {{ ref('stg_airbyte_hubspot_event_signup_web_app') }}

    UNION ALL

    SELECT
        hsga.contact_id,
        "GA4 Session" AS touchpoint_type,
        CONCAT("GA4-", hsga.contact_id, "-", session_id) AS touchpoint_id, -- Including contact_id in case one GA visitor ID matches several HS contact IDs
        session_start_timestamp AS touchpoint_timestamp,
        "session_start" AS web_event_type,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_id,
        utm_content,
        utm_term,
        referrer,
        landing_page,
        gclid,
        fbclid,
        partner_referral_id,
    FROM
        {{ ref('fct_google_analytics_4_sessions') }} AS sess
    INNER JOIN
        hs_contacts_ga_visitors_unique AS hsga
    ON
        sess.user_pseudo_id = hsga.ga_visitor_id
)

SELECT
    tch.contact_id,
    fp.contact_first_page,
    touchpoint_type,
    touchpoint_id,
    touchpoint_timestamp,
    web_event_type,
    landing_page,
    referrer,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN "google"
        WHEN utm_source IS NOT NULL THEN utm_source
        -- Known LLM/IA referrers
        WHEN referrer LIKE "%chatgpt.com%" THEN "chatgpt.com"
        WHEN referrer LIKE "%perplexity.ai%" THEN "perplexity.ai"
        WHEN referrer LIKE "%gemini.google.com" THEN "gemini.google.com"
        WHEN referrer LIKE "%claude.ai%" THEN "claude.ai"
        -- Known organic referrers
        WHEN referrer LIKE "%google.com%" THEN "google"
        WHEN referrer LIKE "%bing.com%" THEN "bing"
        WHEN referrer LIKE "%msn.com%" THEN "msn"
        WHEN referrer LIKE "%yahoo.com%" THEN "yahoo"
        WHEN referrer LIKE "%qwant.com%" THEN "qwant"
        WHEN referrer LIKE "%ecosia.com%" THEN "ecosia"
        WHEN referrer LIKE "%duckduckgo.com%" THEN "duckduckgo"
        WHEN referrer LIKE "%brave.com%" THEN "brave"
        -- Known social referrers
        WHEN referrer LIKE "%youtube.com%" THEN "youtube"
        WHEN referrer LIKE "%linkedin.com%" THEN "linkedin"
        WHEN referrer LIKE "%facebook.com%" THEN "facebook"
        WHEN referrer LIKE "%instagram.com%" THEN "instagram"
        WHEN referrer LIKE "%t.co%" THEN "twitter"
    END AS utm_source,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN "cpc"
        WHEN utm_source IS NOT NULL THEN utm_medium
        -- Known LLM/IA referrers
        WHEN referrer LIKE "%chatgpt.com%" THEN "referral"
        WHEN referrer LIKE "%perplexity.ai%" THEN "referral"
        WHEN referrer LIKE "%gemini.google.com" THEN "referral"
        WHEN referrer LIKE "%claude.ai%" THEN "referral"
        -- Known organic referrers
        WHEN referrer LIKE "%google.com%" THEN "organic"
        WHEN referrer LIKE "%bing.com%" THEN "organic"
        WHEN referrer LIKE "%msn.com%" THEN "organic"
        WHEN referrer LIKE "%yahoo.com%" THEN "organic"
        WHEN referrer LIKE "%qwant.com%" THEN "organic"
        WHEN referrer LIKE "%ecosia.com%" THEN "organic"
        WHEN referrer LIKE "%duckduckgo.com%" THEN "organic"
        WHEN referrer LIKE "%brave.com%" THEN "organic"
        -- Known social referrers
        WHEN referrer LIKE "%youtube.com%" THEN "social"
        WHEN referrer LIKE "%linkedin.com%" THEN "social"
        WHEN referrer LIKE "%facebook.com%" THEN "social"
        WHEN referrer LIKE "%instagram.com%" THEN "social"
        WHEN referrer LIKE "%t.co%" THEN "social"
    END AS utm_medium,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN gcl.campaign_name
        WHEN utm_source IS NOT NULL THEN utm_campaign
    END AS utm_campaign,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN CAST(gcl.campaign_id AS STRING)
        WHEN utm_source IS NOT NULL THEN utm_id
    END AS utm_id,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN gcl.ad_group_name
        WHEN utm_source IS NOT NULL THEN utm_content
    END AS utm_content,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN gcl.keyword_name
        WHEN utm_source IS NOT NULL THEN utm_term
    END AS utm_term,
    tch.gclid,
    fbclid,
    partner_referral_id,
FROM
    all_touchpoints AS tch
LEFT JOIN
    {{ ref('fct_google_ads_clicks') }} AS gcl
ON
    tch.gclid = gcl.gclid
LEFT JOIN
    hs_contacts_first_pages fp
USING
    (contact_id)
