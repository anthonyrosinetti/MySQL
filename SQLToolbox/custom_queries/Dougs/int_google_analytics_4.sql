{{ config(
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
        "field": "date_report",
        "data_type": "date",
        "granularity": "day"
    }
) }}

WITH sessions AS (
    SELECT DISTINCT
        s.date_report,
        s.user_pseudo_id,
        fp.first_page,
        session_id,
        CASE
            WHEN utm_source IN ("partner","partenariat") THEN "Partenariat"
            WHEN utm_source IN ("sponsorship","parrainage") THEN "Parrainage"
            WHEN partner_referral_id IS NOT NULL THEN "Parrainage"
            WHEN utm_source LIKE "%adwords%" THEN "Google Ads"
            WHEN utm_source = "google" AND utm_medium IN ("ads","cpc","ppc","retargeting","cpm","paid") THEN "Google Ads"
            WHEN utm_source = "google" AND utm_medium = "organic" THEN "Google Organic"
            WHEN utm_source = "youtube" THEN "YouTube"
            WHEN utm_source = "linkedin" AND (utm_medium IN ("ads","cpc","ppc","retargeting","cpm") OR utm_medium LIKE "%paid%") THEN "Linkedin Ads"
            WHEN utm_source LIKE "%linkedin%" AND utm_medium = "organic" THEN "Linkedin Organic"
            WHEN utm_source = "tiktok" AND utm_medium IN ("ads","cpc","ppc","retargeting","cpm") THEN "TikTok Ads"
            WHEN utm_source LIKE "%tiktok%" AND utm_medium IN ("organic","social") THEN "TikTok Organic"
            WHEN utm_source LIKE "%paid%" AND utm_medium LIKE "%facebook%" THEN "Meta Ads"
            WHEN utm_source LIKE "%facebook%" AND utm_medium LIKE "%ads%" THEN "Meta Ads"
            WHEN utm_source IN ("facebook","meta","instagram") AND (utm_medium IN ("ads","cpc","ppc","retargeting","cpm") OR utm_medium LIKE "%paid%") THEN "Meta Ads"
            WHEN utm_source = "facebook" AND utm_medium = "social" THEN "Facebook Organic"
            WHEN utm_source IN ("instagram","ig") AND utm_medium = "social" THEN "Instagram Organic"
            WHEN utm_source = "bing" AND utm_medium IN ("ads","cpc","ppc","retargeting","cpm","paid","") THEN "Bing Ads"
            WHEN utm_source = "bing" AND utm_medium = "organic" THEN "Bing Organic"
            WHEN utm_source = "affiliation" THEN "Affiliation"
            WHEN utm_source LIKE "%adyen%" OR utm_source LIKE "%ogone%" OR utm_source LIKE "%getalma%" OR utm_source LIKE "%klarna%" THEN "Payment Service Provider"
            WHEN utm_source = "onsite" THEN "Onsite banner"
            WHEN utm_source = "gmb" THEN "GMB"
            WHEN utm_source LIKE "%hillspet%" THEN "Criteo"
            WHEN utm_source LIKE "%qwant%" AND utm_medium = "referral" THEN "Qwant Search"
            WHEN utm_source LIKE "%bing%" AND utm_medium = "referral" THEN "Bing Search"
            WHEN utm_source LIKE "%google%" AND utm_medium = "referral" THEN "Google Search"
            WHEN utm_source LIKE "%yahoo%" AND utm_medium = "referral" THEN "Yahoo Search"
            WHEN utm_source LIKE "%yahoo%" AND utm_medium = "organic" THEN "Yahoo Organic"
            WHEN utm_source LIKE "%duckduckgo%" AND utm_medium = "referral" THEN "Duckduckgo Search"
            WHEN utm_source LIKE "%duckduckgo%" AND utm_medium = "organic" THEN "Duckduckgo Organic"
            WHEN utm_source LIKE "%ecosia%" AND utm_medium = "referral" THEN "Ecosia Search"
            WHEN utm_source LIKE "%ecosia%" AND utm_medium = "organic" THEN "Ecosia Organic"
            WHEN utm_source LIKE "%baidu%" AND utm_medium = "organic" THEN "Baidu Organic"
            WHEN utm_source LIKE "%brave%" AND utm_medium = "referral" THEN "Brave Search"
            WHEN utm_medium = "organic" THEN "SEO"
            WHEN utm_source LIKE "%twitch%" AND utm_medium = "social" THEN "Twitch Organic"
            WHEN utm_source LIKE "%twitter%" AND utm_medium = "social" THEN "Twitter Organic"
            WHEN utm_source LIKE "%reddit%" AND utm_medium = "social" THEN "Reddit Organic"
            WHEN utm_source LIKE "%tiktok%" AND utm_medium = "referral" THEN "Tiktok Organic"
            WHEN utm_source LIKE "%meta%" AND utm_medium = "referral" THEN "Facebook Organic"
            WHEN utm_source LIKE "%facebook%" AND utm_medium = "referral" THEN "Facebook Organic"
            WHEN utm_source LIKE "%instagram%" AND utm_medium = "referral" THEN "Instagram Organic"
            WHEN utm_source LIKE "%snapchat%" AND utm_medium = "referral" THEN "Snapchat Organic"
            WHEN utm_source LIKE "%pinterest%" AND utm_medium = "referral" THEN "Pinterest Organic"
            WHEN utm_source LIKE "%twitter%" AND utm_medium = "referral" THEN "Twitter Organic"
            WHEN utm_source LIKE "%linkedin%" AND utm_medium = "referral" THEN "Linkedin Organic"
            WHEN utm_source LIKE "%chatgpt%" THEN "AI Referrals"
            WHEN utm_source LIKE "%perplexity%" THEN "AI Referrals"
            WHEN utm_medium LIKE "%referral%" THEN "Referral Websites"        
            WHEN utm_medium LIKE "%webinar%" THEN "Webinar"
            WHEN utm_source = "direct" AND utm_medium = "none" THEN "Direct"
            WHEN utm_source = "(direct)" AND utm_medium = "(none)" THEN "Direct"
            WHEN utm_source LIKE "%mail%" THEN "Email"
            WHEN utm_medium LIKE "%mail%" THEN "Email"
            WHEN utm_source = "hs_automation" THEN "Email"
        END AS session_source_name,
        CASE
            WHEN utm_source IN ("partner","partenariat") THEN "Partenariat"
            WHEN utm_source IN ("sponsorship","parrainage") THEN "Parrainage"
            WHEN partner_referral_id IS NOT NULL THEN "Parrainage"
            WHEN (utm_source LIKE "%adwords%") OR (utm_source IN ("google","bing") AND utm_medium IN ("ads","cpc","ppc","retargeting","cpm","paid")) THEN "Paid Search"
            WHEN utm_source = "google" AND utm_medium = "organic" THEN "SEO"
            WHEN utm_source = "youtube" AND utm_medium LIKE "%paid%" THEN "Paid Social"
            WHEN utm_source = "youtube" THEN "Organic Social"
            WHEN utm_source IN ("linkedin","tiktok","facebook","meta","instagram") AND (utm_medium IN ("ads","cpc","ppc","retargeting","cpm") OR utm_medium LIKE "%paid%") THEN "Paid Social"
            WHEN utm_source LIKE "%paid%" AND utm_medium LIKE "%facebook%" THEN "Paid Social"
            WHEN utm_source LIKE "%facebook%" AND utm_medium LIKE "%ads%" THEN "Paid Social"
            WHEN utm_source LIKE "%linkedin%" AND utm_medium = "organic" THEN "Organic Social"
            WHEN utm_source LIKE "%tiktok%" AND utm_medium IN ("organic","social") THEN "Organic Social"
            WHEN utm_source IN ("facebook","instagram","ig") AND utm_medium = "social" THEN "Organic Social"        
            WHEN utm_source = "bing" AND utm_medium = "organic" THEN "SEO"
            WHEN utm_source = "affiliation" THEN "Affiliation"
            WHEN utm_source LIKE "%adyen%" OR utm_source LIKE "%ogone%" OR utm_source LIKE "%getalma%" OR utm_source LIKE "%klarna%" THEN "Payment Service Provider"
            WHEN utm_source = "onsite" THEN "Onsite banner"
            WHEN utm_source = "gmb" THEN "GMB"
            WHEN utm_source LIKE "%hillspet%" THEN "Criteo"
            WHEN utm_source LIKE "%qwant%" AND utm_medium = "referral" THEN "SEO"
            WHEN utm_source LIKE "%bing%" AND utm_medium = "referral" THEN "SEO"
            WHEN utm_source LIKE "%google%" AND utm_medium = "referral" THEN "SEO"
            WHEN utm_source LIKE "%yahoo%" AND utm_medium = "referral" THEN "SEO"
            WHEN utm_source LIKE "%yahoo%" AND utm_medium = "organic" THEN "SEO"
            WHEN utm_source LIKE "%duckduckgo%" AND utm_medium = "referral" THEN "SEO"
            WHEN utm_source LIKE "%duckduckgo%" AND utm_medium = "organic" THEN "SEO"
            WHEN utm_source LIKE "%ecosia%" AND utm_medium = "referral" THEN "SEO"
            WHEN utm_source LIKE "%ecosia%" AND utm_medium = "organic" THEN "SEO"
            WHEN utm_source LIKE "%baidu%" AND utm_medium = "organic" THEN "SEO"
            WHEN utm_source LIKE "%brave%" AND utm_medium = "referral" THEN "SEO"
            WHEN utm_medium = "organic" THEN "SEO"
            WHEN utm_source LIKE "%twitch%" AND utm_medium = "social" THEN "Organic Social"
            WHEN utm_source LIKE "%twitter%" AND utm_medium = "social" THEN "Organic Social"
            WHEN utm_source LIKE "%reddit%" AND utm_medium = "social" THEN "Organic Social"
            WHEN utm_source LIKE "%tiktok%" AND utm_medium = "referral" THEN "Organic Social"
            WHEN utm_source LIKE "%meta%" AND utm_medium = "referral" THEN "Organic Social"
            WHEN utm_source LIKE "%facebook%" AND utm_medium = "referral" THEN "Organic Social"
            WHEN utm_source LIKE "%instagram%" AND utm_medium = "referral" THEN "Organic Social"
            WHEN utm_source LIKE "%snapchat%" AND utm_medium = "referral" THEN "Organic Social"
            WHEN utm_source LIKE "%pinterest%" AND utm_medium = "referral" THEN "Organic Social"
            WHEN utm_source LIKE "%twitter%" AND utm_medium = "referral" THEN "Organic Social"
            WHEN utm_source LIKE "%linkedin%" AND utm_medium = "referral" THEN "Organic Social"
            WHEN utm_source LIKE "%chatgpt%" THEN "AI Referrals"
            WHEN utm_source LIKE "%perplexity%" THEN "AI Referrals"
            WHEN utm_medium LIKE "%referral%" THEN "Referral Websites"        
            WHEN utm_medium LIKE "%webinar%" THEN "Webinar"
            WHEN utm_source = "direct" AND utm_medium = "none" THEN "Direct"
            WHEN utm_source = "(direct)" AND utm_medium = "(none)" THEN "Direct"
            WHEN utm_source LIKE "%mail%" THEN "Email"
            WHEN utm_medium LIKE "%mail%" THEN "Email"
            WHEN utm_source = "hs_automation" THEN "Email"
        END AS session_channel,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_id,
        utm_content,
        utm_term,
        partner_referral_id
    FROM
        {{ ref('fct_google_analytics_4_sessions') }} s
        LEFT JOIN
            {{ ref('fct_google_analytics_4_first_pageviews') }} fp
        USING
            (user_pseudo_id)
    WHERE
        TRUE
        -- Due to the fact that table contains a important amount of data, we work on a 3-day rolling window to update the data
        {% if is_incremental() %}
        AND DATE(session_start_timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY) AND CURRENT_DATE()
        {% endif %}
)
SELECT
    *
FROM
    sessions
