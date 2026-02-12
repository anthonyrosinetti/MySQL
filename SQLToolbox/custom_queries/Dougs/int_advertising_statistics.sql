WITH all_platforms AS (
    SELECT
        st.date_report,
        "Google Ads" AS source_name,
        "Paid Search" AS channel,
        st.customer_id AS account_id,
        ca.campaign_type,
        st.campaign_id,
        ca.campaign_name,
        st.ad_group_id,
        ag.ad_group_name,
        st.ad_id,
        ad.ad_name,
        st.keyword_id,
        kw.keyword_name,
        st.impressions,
        st.clicks,
        st.spend
    FROM
        {{ ref('fct_google_ads') }} AS st
    LEFT JOIN
        {{ ref('dim_google_ads_campaigns') }} AS ca
    ON
        st.campaign_id = ca.campaign_id
    LEFT JOIN
        {{ ref('dim_google_ads_ad_groups') }} AS ag
    ON
        st.ad_group_id = ag.ad_group_id
    LEFT JOIN
        {{ ref('dim_google_ads_ads') }} AS ad
    ON
        st.ad_group_id = ad.ad_group_id
        AND st.ad_id = ad.ad_id
    LEFT JOIN
        {{ ref('dim_google_ads_keywords') }} AS kw
    ON
        st.ad_group_id = kw.ad_group_id
        AND st.keyword_id = kw.keyword_id

    UNION ALL

    SELECT
        st.date_report,
        "LinkedIn Ads" AS source_name,
        "Paid Social" AS channel,
        st.account_id,
        CAST(NULL AS STRING) AS campaign_type,
        st.campaign_group_id AS campaign_id,
        cg.campaign_group_name AS campaign_name,
        st.campaign_id AS ad_group_id,
        ca.campaign_name AS ad_group_name,
        st.creative_id AS ad_id,
        cr.creative_name AS ad_name,
        CAST(NULL AS STRING) AS keyword_id,
        CAST(NULL AS STRING) AS keyword_name,
        st.impressions,
        st.clicks,
        st.spend
    FROM
        {{ ref('fct_linkedin_ads') }} AS st
    LEFT JOIN
        {{ ref('dim_linkedin_ads_campaign_groups') }} AS cg
    ON
        st.campaign_group_id = cg.campaign_group_id
    LEFT JOIN
        {{ ref('dim_linkedin_ads_campaigns') }} AS ca
    ON
        st.campaign_id = ca.campaign_id
    LEFT JOIN
        {{ ref('dim_linkedin_ads_creatives') }} AS cr
    ON
        st.creative_id = cr.creative_id

    UNION ALL

    SELECT
        st.date_report,
        "Meta Ads" AS source_name,
        "Paid Social" AS channel,
        st.account_id,
        CAST(NULL AS STRING) AS campaign_type,
        st.campaign_id,
        ca.campaign_name,
        st.adset_id AS ad_group_id,
        ag.adset_name AS ad_group_name,
        st.ad_id,
        ad.ad_name,
        CAST(NULL AS STRING) AS keyword_id,
        CAST(NULL AS STRING) AS keyword_name,
        st.impressions,
        st.clicks,
        st.spend
    FROM
        {{ ref('fct_meta_ads') }} AS st
    LEFT JOIN
        {{ ref('dim_meta_ads_campaigns') }} AS ca
    ON
        st.campaign_id = ca.campaign_id
    LEFT JOIN
        {{ ref('dim_meta_ads_adsets') }} AS ag
    ON
        st.adset_id = ag.adset_id
    LEFT JOIN
        {{ ref('dim_meta_ads_ads') }} AS ad
    ON
        st.ad_id = ad.ad_id

    UNION ALL

    SELECT
        st.date_report,
        "Bing Ads" AS source_name,
        "Paid Search" AS channel,
        st.account_id,
        ca.campaign_type,
        st.campaign_id,
        ca.campaign_name,
        st.ad_group_id,
        ag.ad_group_name,
        st.ad_id,
        ad.ad_name,
        st.keyword_id,
        kw.keyword_name,
        st.impressions,
        st.clicks,
        st.spend
    FROM
        {{ ref('fct_bing_ads') }} AS st
    LEFT JOIN
        {{ ref('dim_bing_ads_campaigns') }} AS ca
    ON
        st.campaign_id = ca.campaign_id
    LEFT JOIN
        {{ ref('dim_bing_ads_ad_groups') }} AS ag
    ON
        st.ad_group_id = ag.ad_group_id
    LEFT JOIN
        {{ ref('dim_bing_ads_ads') }} AS ad
    ON
        st.ad_id = ad.ad_id
    LEFT JOIN
        {{ ref('dim_bing_ads_keywords') }} AS kw
    ON
        st.keyword_id = kw.keyword_id

    UNION ALL

    SELECT
        st.date_report,
        "TikTok Ads" AS source_name,
        "Paid Social" AS channel,
        CAST(NULL AS STRING) AS account_id,
        CAST(NULL AS STRING) AS campaign_type,
        st.campaign_id,
        ca.campaign_name,
        st.adgroup_id AS ad_group_id,
        ag.adgroup_name AS ad_group_name,
        st.ad_id,
        ad.ad_name,
        CAST(NULL AS STRING) AS keyword_id,
        CAST(NULL AS STRING) AS keyword_name,
        st.impressions,
        st.clicks,
        st.spend
    FROM
        {{ ref('fct_tiktok_ads') }} AS st
    LEFT JOIN
        {{ ref('dim_tiktok_ads_campaigns') }} AS ca
    ON
        st.campaign_id = ca.campaign_id
    LEFT JOIN
        {{ ref('dim_tiktok_ads_adgroups') }} AS ag
    ON
        st.adgroup_id = ag.adgroup_id
    LEFT JOIN
        {{ ref('dim_tiktok_ads_ads') }} AS ad
    ON
        st.ad_id = ad.ad_id
)

SELECT
    date_report,
    source_name,
    channel,
    account_id,
    campaign_type,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    ad_id,
    ad_name,
    keyword_id,
    keyword_name,
    NULLIF(SUM(impressions), 0) AS impressions,
    NULLIF(SUM(clicks), 0) AS clicks,
    NULLIF(SUM(spend), 0) AS spend
FROM
    all_platforms
GROUP BY
    date_report,
    source_name,
    channel,
    account_id,
    campaign_type,
    campaign_id,
    campaign_name,
    ad_group_id,
    ad_group_name,
    ad_id,
    ad_name,
    keyword_id,
    keyword_name
HAVING
    impressions IS NOT NULL
    OR clicks IS NOT NULL
    OR spend IS NOT NULL
