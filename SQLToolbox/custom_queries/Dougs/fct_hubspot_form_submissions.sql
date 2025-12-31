WITH hs_contacts AS ( -- Keeping only the last contact_id per email address ; previous ones have probably been deleted or merged
    SELECT
        email,
        hs_contact_id,
    FROM
        {{ ref('dim_hubspot_contacts') }}
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY email
            ORDER BY updated_at_timestamp DESC, created_at_timestamp DESC
        ) = 1
)

SELECT
    hfs.form_submission_id,
    hfs.form_id,
    hfo.form_name,
    hfo.form_type,
    COALESCE(hev.hs_contact_id, hco.hs_contact_id) AS contact_id,
    TIMESTAMP_MILLIS(hfs.submission_timestamp) AS submission_timestamp,
    hfs.page_url,
    hfs.email_address,
    hfs.product_type,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN "google"
        WHEN hfs.session_utm_source IS NOT NULL THEN hfs.session_utm_source
        WHEN hfs.utm_source IS NOT NULL THEN hfs.utm_source
    END AS utm_source,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN "cpc"
        WHEN hfs.session_utm_source IS NOT NULL THEN hfs.session_utm_medium
        WHEN hfs.utm_source IS NOT NULL THEN hfs.utm_medium
    END AS utm_medium,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN gcl.campaign_name
        WHEN hfs.session_utm_source IS NOT NULL THEN hfs.session_utm_campaign
        WHEN hfs.utm_source IS NOT NULL THEN hfs.utm_campaign
    END AS utm_campaign,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN CAST(gcl.campaign_id AS STRING)
        WHEN hfs.session_utm_source IS NOT NULL THEN hfs.session_utm_id
        WHEN hfs.utm_source IS NOT NULL THEN CAST(NULL AS STRING)
    END AS utm_id,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN gcl.ad_group_name
        WHEN hfs.session_utm_source IS NOT NULL THEN hfs.session_utm_content
        WHEN hfs.utm_source IS NOT NULL THEN hfs.utm_content
    END AS utm_content,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN gcl.keyword_name
        WHEN hfs.session_utm_source IS NOT NULL THEN hfs.session_utm_term
        WHEN hfs.utm_source IS NOT NULL THEN hfs.utm_term
    END AS utm_term,
    CASE
        WHEN gcl.gclid IS NOT NULL THEN CAST(NULL AS STRING)
        WHEN hfs.session_utm_source IS NOT NULL THEN CAST(NULL AS STRING)
        WHEN hfs.utm_source IS NOT NULL THEN CAST(NULL AS STRING)
        ELSE session_referrer
    END AS referrer,
    REGEXP_EXTRACT(hfs.session_landing_page, r"^(?:https?:\/\/)?(\/[^?#]*)(?:[?#].*)?$") AS landing_page,
    hfs.session_gclid AS gclid,
    REGEXP_EXTRACT(hfs.session_landing_page, r"[\?&]fbclid=([^&]*)") AS fbclid,
    REGEXP_EXTRACT(hfs.session_landing_page, r"[\?&]r=([^&]*)") AS partner_referral_id,
    hfs.session_ga_visitor_id AS ga_visitor_id
FROM
    {{ ref('stg_airbyte_hubspot_form_submissions') }} AS hfs
LEFT JOIN
    {{ ref('stg_airbyte_hubspot_event_form_submission_v2') }} AS hev
ON
    hfs.form_submission_id = hev.hs_event_id
LEFT JOIN
    hs_contacts AS hco
ON
    hfs.email_address = hco.email
LEFT JOIN
    {{ ref('dim_hubspot_forms') }} AS hfo
ON
    hfs.form_id = hfo.form_id
LEFT JOIN
    {{ ref('fct_google_ads_clicks') }} AS gcl
ON
    hfs.session_gclid = gcl.gclid
