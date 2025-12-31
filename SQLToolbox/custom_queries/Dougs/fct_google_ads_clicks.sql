WITH gclids AS (
    SELECT
        customer_id,
        campaign_id,
        ad_group_id,
        SAFE_CAST(REGEXP_EXTRACT(ad_group_id_ad_id, r"^customers/[0-9]+/adGroupAds/[0-9]+~([0-9]+)$") AS INT64) AS ad_id,
        SAFE_CAST(REGEXP_EXTRACT(ad_group_id_keyword_id, r"^customers/[0-9]+/adGroupCriteria/[0-9]+~([0-9]+)$") AS INT64) AS keyword_id,
        gclid
    FROM
        {{ ref('stg_google_ads_click_stats') }}
    WHERE
        gclid IS NOT NULL
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY gclid
            ORDER BY date DESC
        ) = 1
)

SELECT
    gcl.customer_id,
    gcl.campaign_id,
    cam.campaign_name,
    gcl.ad_group_id,
    adg.ad_group_name,
    gcl.ad_id,
    ad.ad_name,
    gcl.keyword_id,
    kw.keyword_name,
    gcl.gclid
FROM
    gclids AS gcl
LEFT JOIN
    {{ ref('dim_google_ads_campaigns') }} AS cam
ON
    gcl.campaign_id = cam.campaign_id
LEFT JOIN
    {{ ref('dim_google_ads_ad_groups') }} AS adg
ON
    gcl.ad_group_id = adg.ad_group_id
LEFT JOIN
    {{ ref('dim_google_ads_ads') }} AS ad
ON
    gcl.ad_group_id = ad.ad_group_id
    AND gcl.ad_id = ad.ad_id
LEFT JOIN
    {{ ref('dim_google_ads_keywords') }} AS kw
ON
    gcl.ad_group_id = kw.ad_group_id
    AND gcl.keyword_id = kw.keyword_id
