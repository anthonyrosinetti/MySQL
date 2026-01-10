WITH paid_prestations AS (
  SELECT DISTINCT
    company_id,
    DATE(invoice_issued_at) AS date,
    prestation_name_standard,
    prestation_amount,
    prestation_category,
    data_prestation_category
  FROM
    {{ ref('int_prestations') }}
  WHERE
    prestation_is_paid
    AND
    (
    data_prestation_category = 'Ponctuel'
    OR
      prestation_category IN (
        'subscription',
        'discount'
      )
    ) 
),
contact_stages AS (
  SELECT
    contact_id,
    dougs_user_id,
    company_id,
    dougs_company_id,
    gender,
    age,
    eligible,
    legal_form,
    ape_code,
    activity,
    first_conversion_form,
    first_conversion_form_category,
    first_conversion_form_type,
    contact_category,
    lost_reason,
    contact_type,
    treatment_type,
    first_touchpoint_timestamp,
    CASE
        WHEN first_touchpoint_source IN ('partner','partenariat') THEN "Partenariat"
        WHEN first_touchpoint_source IN ('sponsorship','parrainage') THEN "Parrainage"
        WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
        WHEN first_touchpoint_source LIKE '%adwords%' THEN "Google Ads"
        WHEN first_touchpoint_source = 'google' AND first_touchpoint_medium IN ('ads','cpc','ppc','retargeting','cpm','paid') THEN "Google Ads"
        WHEN first_touchpoint_source = 'google' AND first_touchpoint_medium = 'organic' THEN "Google Organic"
        WHEN first_touchpoint_source = 'youtube' THEN "YouTube"
        WHEN first_touchpoint_source = 'linkedin' AND (first_touchpoint_medium IN ('ads','cpc','ppc','retargeting','cpm') OR first_touchpoint_medium LIKE '%paid%') THEN "Linkedin Ads"
        WHEN first_touchpoint_source LIKE '%linkedin%' AND first_touchpoint_medium = 'organic' THEN "Linkedin Organic"
        WHEN first_touchpoint_source = 'tiktok' AND first_touchpoint_medium IN ('ads','cpc','ppc','retargeting','cpm') THEN "TikTok Ads"
        WHEN first_touchpoint_source LIKE '%tiktok%' AND first_touchpoint_medium IN ('organic','social') THEN "TikTok Organic"
        WHEN first_touchpoint_source LIKE '%paid%' AND first_touchpoint_medium LIKE '%facebook%' THEN "Meta Ads"
        WHEN first_touchpoint_source LIKE '%facebook%' AND first_touchpoint_medium LIKE '%ads%' THEN "Meta Ads"
        WHEN first_touchpoint_source IN ('facebook','meta','instagram') AND (first_touchpoint_medium IN ('ads','cpc','ppc','retargeting','cpm') OR first_touchpoint_medium LIKE '%paid%') THEN "Meta Ads"
        WHEN first_touchpoint_source = 'facebook' AND first_touchpoint_medium = 'social' THEN "Facebook Organic"
        WHEN first_touchpoint_source IN ('instagram','ig') AND first_touchpoint_medium = 'social' THEN "Instagram Organic"
        WHEN first_touchpoint_source = 'bing' AND first_touchpoint_medium IN ('ads','cpc','ppc','retargeting','cpm','paid','') THEN "Bing Ads"
        WHEN first_touchpoint_source = 'bing' AND first_touchpoint_medium = 'organic' THEN "Bing Organic"
        WHEN first_touchpoint_source = 'affiliation' THEN "Affiliation"
        WHEN first_touchpoint_source LIKE '%adyen%' OR first_touchpoint_source LIKE '%ogone%' OR first_touchpoint_source LIKE '%getalma%' OR first_touchpoint_source LIKE '%klarna%' THEN "Payment Service Provider"
        WHEN first_touchpoint_source = 'onsite' THEN "Onsite banner"
        WHEN first_touchpoint_source = 'gmb' THEN "GMB"
        WHEN first_touchpoint_source LIKE '%hillspet%' THEN "Criteo"
        WHEN first_touchpoint_source LIKE '%qwant%' AND first_touchpoint_medium = 'referral' THEN "Qwant Search"
        WHEN first_touchpoint_source LIKE '%bing%' AND first_touchpoint_medium = 'referral' THEN "Bing Search"
        WHEN first_touchpoint_source LIKE '%google%' AND first_touchpoint_medium = 'referral' THEN "Google Search"
        WHEN first_touchpoint_source LIKE '%yahoo%' AND first_touchpoint_medium = 'referral' THEN "Yahoo Search"
        WHEN first_touchpoint_source LIKE '%yahoo%' AND first_touchpoint_medium = 'organic' THEN "Yahoo Organic"
        WHEN first_touchpoint_source LIKE '%duckduckgo%' AND first_touchpoint_medium = 'referral' THEN "Duckduckgo Search"
        WHEN first_touchpoint_source LIKE '%duckduckgo%' AND first_touchpoint_medium = 'organic' THEN "Duckduckgo Organic"
        WHEN first_touchpoint_source LIKE '%ecosia%' AND first_touchpoint_medium = 'referral' THEN "Ecosia Search"
        WHEN first_touchpoint_source LIKE '%ecosia%' AND first_touchpoint_medium = 'organic' THEN "Ecosia Organic"
        WHEN first_touchpoint_source LIKE '%baidu%' AND first_touchpoint_medium = 'organic' THEN "Baidu Organic"
        WHEN first_touchpoint_source LIKE '%brave%' AND first_touchpoint_medium = 'referral' THEN "Brave Search"
        WHEN first_touchpoint_medium = 'organic' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%twitch%' AND first_touchpoint_medium = 'social' THEN "Twitch Organic"
        WHEN first_touchpoint_source LIKE '%twitter%' AND first_touchpoint_medium = 'social' THEN "Twitter Organic"
        WHEN first_touchpoint_source LIKE '%reddit%' AND first_touchpoint_medium = 'social' THEN "Reddit Organic"
        WHEN first_touchpoint_source LIKE '%tiktok%' AND first_touchpoint_medium = 'referral' THEN "Tiktok Organic"
        WHEN first_touchpoint_source LIKE '%meta%' AND first_touchpoint_medium = 'referral' THEN "Facebook Organic"
        WHEN first_touchpoint_source LIKE '%facebook%' AND first_touchpoint_medium = 'referral' THEN "Facebook Organic"
        WHEN first_touchpoint_source LIKE '%instagram%' AND first_touchpoint_medium = 'referral' THEN "Instagram Organic"
        WHEN first_touchpoint_source LIKE '%snapchat%' AND first_touchpoint_medium = 'referral' THEN "Snapchat Organic"
        WHEN first_touchpoint_source LIKE '%pinterest%' AND first_touchpoint_medium = 'referral' THEN "Pinterest Organic"
        WHEN first_touchpoint_source LIKE '%twitter%' AND first_touchpoint_medium = 'referral' THEN "Twitter Organic"
        WHEN first_touchpoint_source LIKE '%linkedin%' AND first_touchpoint_medium = 'referral' THEN "Linkedin Organic"
        WHEN first_touchpoint_source LIKE '%chatgpt%' THEN "AI Referrals"
        WHEN first_touchpoint_source LIKE '%perplexity%' THEN "AI Referrals"
        WHEN first_touchpoint_medium LIKE '%referral%' THEN "Referral Websites"        
        WHEN first_touchpoint_medium LIKE '%webinar%' THEN "Webinar"
        WHEN first_touchpoint_source = 'direct' AND first_touchpoint_medium = 'none' THEN "Direct"
        WHEN first_touchpoint_source = '(direct)' AND first_touchpoint_medium = '(none)' THEN "Direct"
        WHEN first_touchpoint_source LIKE '%mail%' THEN "Email"
        WHEN first_touchpoint_medium LIKE '%mail%' THEN "Email"
        WHEN first_touchpoint_source = 'hs_automation' THEN "Email"
    END AS first_touchpoint_source_name,
    CASE
        WHEN first_touchpoint_source IN ('partner','partenariat') THEN "Partenariat"
        WHEN first_touchpoint_source IN ('sponsorship','parrainage') THEN "Parrainage"
        WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
        WHEN (first_touchpoint_source LIKE '%adwords%') OR (first_touchpoint_source IN ('google','bing') AND first_touchpoint_medium IN ('ads','cpc','ppc','retargeting','cpm','paid')) THEN "Paid Search"
        WHEN first_touchpoint_source = 'google' AND first_touchpoint_medium = 'organic' THEN "SEO"
        WHEN first_touchpoint_source = 'youtube' AND first_touchpoint_medium LIKE '%paid%' THEN "Paid Social"
        WHEN first_touchpoint_source = 'youtube' THEN "Organic Social"
        WHEN first_touchpoint_source IN ('linkedin','tiktok','facebook','meta','instagram') AND (first_touchpoint_medium IN ('ads','cpc','ppc','retargeting','cpm') OR first_touchpoint_medium LIKE '%paid%') THEN "Paid Social"
        WHEN first_touchpoint_source LIKE '%paid%' AND first_touchpoint_medium LIKE '%facebook%' THEN "Paid Social"
        WHEN first_touchpoint_source LIKE '%facebook%' AND first_touchpoint_medium LIKE '%ads%' THEN "Paid Social"
        WHEN first_touchpoint_source LIKE '%linkedin%' AND first_touchpoint_medium = 'organic' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%tiktok%' AND first_touchpoint_medium IN ('organic','social') THEN "Organic Social"
        WHEN first_touchpoint_source IN ('facebook','instagram','ig') AND first_touchpoint_medium = 'social' THEN "Organic Social"        
        WHEN first_touchpoint_source = 'bing' AND first_touchpoint_medium = 'organic' THEN "SEO"
        WHEN first_touchpoint_source = 'affiliation' THEN "Affiliation"
        WHEN first_touchpoint_source LIKE '%adyen%' OR first_touchpoint_source LIKE '%ogone%' OR first_touchpoint_source LIKE '%getalma%' OR first_touchpoint_source LIKE '%klarna%' THEN "Payment Service Provider"
        WHEN first_touchpoint_source = 'onsite' THEN "Onsite banner"
        WHEN first_touchpoint_source = 'gmb' THEN "GMB"
        WHEN first_touchpoint_source LIKE '%hillspet%' THEN "Criteo"
        WHEN first_touchpoint_source LIKE '%qwant%' AND first_touchpoint_medium = 'referral' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%bing%' AND first_touchpoint_medium = 'referral' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%google%' AND first_touchpoint_medium = 'referral' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%yahoo%' AND first_touchpoint_medium = 'referral' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%yahoo%' AND first_touchpoint_medium = 'organic' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%duckduckgo%' AND first_touchpoint_medium = 'referral' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%duckduckgo%' AND first_touchpoint_medium = 'organic' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%ecosia%' AND first_touchpoint_medium = 'referral' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%ecosia%' AND first_touchpoint_medium = 'organic' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%baidu%' AND first_touchpoint_medium = 'organic' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%brave%' AND first_touchpoint_medium = 'referral' THEN "SEO"
        WHEN first_touchpoint_medium = 'organic' THEN "SEO"
        WHEN first_touchpoint_source LIKE '%twitch%' AND first_touchpoint_medium = 'social' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%twitter%' AND first_touchpoint_medium = 'social' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%reddit%' AND first_touchpoint_medium = 'social' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%tiktok%' AND first_touchpoint_medium = 'referral' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%meta%' AND first_touchpoint_medium = 'referral' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%facebook%' AND first_touchpoint_medium = 'referral' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%instagram%' AND first_touchpoint_medium = 'referral' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%snapchat%' AND first_touchpoint_medium = 'referral' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%pinterest%' AND first_touchpoint_medium = 'referral' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%twitter%' AND first_touchpoint_medium = 'referral' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%linkedin%' AND first_touchpoint_medium = 'referral' THEN "Organic Social"
        WHEN first_touchpoint_source LIKE '%chatgpt%' THEN "AI Referrals"
        WHEN first_touchpoint_source LIKE '%perplexity%' THEN "AI Referrals"
        WHEN first_touchpoint_medium LIKE '%referral%' THEN "Referral Websites"        
        WHEN first_touchpoint_medium LIKE '%webinar%' THEN "Webinar"
        WHEN first_touchpoint_source = 'direct' AND first_touchpoint_medium = 'none' THEN "Direct"
        WHEN first_touchpoint_source = '(direct)' AND first_touchpoint_medium = '(none)' THEN "Direct"
        WHEN first_touchpoint_source LIKE '%mail%' THEN "Email"
        WHEN first_touchpoint_medium LIKE '%mail%' THEN "Email"
        WHEN first_touchpoint_source = 'hs_automation' THEN "Email"
    END AS first_touchpoint_channel,
    first_touchpoint_campaign,
    first_form_timestamp,
        CASE
        WHEN first_form_source IN ('partner','partenariat') THEN "Partenariat"
        WHEN first_form_source IN ('sponsorship','parrainage') THEN "Parrainage"
        WHEN first_form_partner_referral_id IS NOT NULL THEN "Parrainage"
        WHEN first_form_source LIKE '%adwords%' THEN "Google Ads"
        WHEN first_form_source = 'google' AND first_form_medium IN ('ads','cpc','ppc','retargeting','cpm','paid') THEN "Google Ads"
        WHEN first_form_source = 'google' AND first_form_medium = 'organic' THEN "Google Organic"
        WHEN first_form_source = 'youtube' THEN "YouTube"
        WHEN first_form_source = 'linkedin' AND (first_form_medium IN ('ads','cpc','ppc','retargeting','cpm') OR first_form_medium LIKE '%paid%') THEN "Linkedin Ads"
        WHEN first_form_source LIKE '%linkedin%' AND first_form_medium = 'organic' THEN "Linkedin Organic"
        WHEN first_form_source = 'tiktok' AND first_form_medium IN ('ads','cpc','ppc','retargeting','cpm') THEN "TikTok Ads"
        WHEN first_form_source LIKE '%tiktok%' AND first_form_medium IN ('organic','social') THEN "TikTok Organic"
        WHEN first_form_source LIKE '%paid%' AND first_form_medium LIKE '%facebook%' THEN "Meta Ads"
        WHEN first_form_source LIKE '%facebook%' AND first_form_medium LIKE '%ads%' THEN "Meta Ads"
        WHEN first_form_source IN ('facebook','meta','instagram') AND (first_form_medium IN ('ads','cpc','ppc','retargeting','cpm') OR first_form_medium LIKE '%paid%') THEN "Meta Ads"
        WHEN first_form_source = 'facebook' AND first_form_medium = 'social' THEN "Facebook Organic"
        WHEN first_form_source IN ('instagram','ig') AND first_form_medium = 'social' THEN "Instagram Organic"
        WHEN first_form_source = 'bing' AND first_form_medium IN ('ads','cpc','ppc','retargeting','cpm','paid','') THEN "Bing Ads"
        WHEN first_form_source = 'bing' AND first_form_medium = 'organic' THEN "Bing Organic"
        WHEN first_form_source = 'affiliation' THEN "Affiliation"
        WHEN first_form_source LIKE '%adyen%' OR first_form_source LIKE '%ogone%' OR first_form_source LIKE '%getalma%' OR first_form_source LIKE '%klarna%' THEN "Payment Service Provider"
        WHEN first_form_source = 'onsite' THEN "Onsite banner"
        WHEN first_form_source = 'gmb' THEN "GMB"
        WHEN first_form_source LIKE '%hillspet%' THEN "Criteo"
        WHEN first_form_source LIKE '%qwant%' AND first_form_medium = 'referral' THEN "Qwant Search"
        WHEN first_form_source LIKE '%bing%' AND first_form_medium = 'referral' THEN "Bing Search"
        WHEN first_form_source LIKE '%google%' AND first_form_medium = 'referral' THEN "Google Search"
        WHEN first_form_source LIKE '%yahoo%' AND first_form_medium = 'referral' THEN "Yahoo Search"
        WHEN first_form_source LIKE '%yahoo%' AND first_form_medium = 'organic' THEN "Yahoo Organic"
        WHEN first_form_source LIKE '%duckduckgo%' AND first_form_medium = 'referral' THEN "Duckduckgo Search"
        WHEN first_form_source LIKE '%duckduckgo%' AND first_form_medium = 'organic' THEN "Duckduckgo Organic"
        WHEN first_form_source LIKE '%ecosia%' AND first_form_medium = 'referral' THEN "Ecosia Search"
        WHEN first_form_source LIKE '%ecosia%' AND first_form_medium = 'organic' THEN "Ecosia Organic"
        WHEN first_form_source LIKE '%baidu%' AND first_form_medium = 'organic' THEN "Baidu Organic"
        WHEN first_form_source LIKE '%brave%' AND first_form_medium = 'referral' THEN "Brave Search"
        WHEN first_form_medium = 'organic' THEN "SEO"
        WHEN first_form_source LIKE '%twitch%' AND first_form_medium = 'social' THEN "Twitch Organic"
        WHEN first_form_source LIKE '%twitter%' AND first_form_medium = 'social' THEN "Twitter Organic"
        WHEN first_form_source LIKE '%reddit%' AND first_form_medium = 'social' THEN "Reddit Organic"
        WHEN first_form_source LIKE '%tiktok%' AND first_form_medium = 'referral' THEN "Tiktok Organic"
        WHEN first_form_source LIKE '%meta%' AND first_form_medium = 'referral' THEN "Facebook Organic"
        WHEN first_form_source LIKE '%facebook%' AND first_form_medium = 'referral' THEN "Facebook Organic"
        WHEN first_form_source LIKE '%instagram%' AND first_form_medium = 'referral' THEN "Instagram Organic"
        WHEN first_form_source LIKE '%snapchat%' AND first_form_medium = 'referral' THEN "Snapchat Organic"
        WHEN first_form_source LIKE '%pinterest%' AND first_form_medium = 'referral' THEN "Pinterest Organic"
        WHEN first_form_source LIKE '%twitter%' AND first_form_medium = 'referral' THEN "Twitter Organic"
        WHEN first_form_source LIKE '%linkedin%' AND first_form_medium = 'referral' THEN "Linkedin Organic"
        WHEN first_form_source LIKE '%chatgpt%' THEN "AI Referrals"
        WHEN first_form_source LIKE '%perplexity%' THEN "AI Referrals"
        WHEN first_form_medium LIKE '%referral%' THEN "Referral Websites"        
        WHEN first_form_medium LIKE '%webinar%' THEN "Webinar"
        WHEN first_form_source = 'direct' AND first_form_medium = 'none' THEN "Direct"
        WHEN first_form_source = '(direct)' AND first_form_medium = '(none)' THEN "Direct"
        WHEN first_form_source LIKE '%mail%' THEN "Email"
        WHEN first_form_medium LIKE '%mail%' THEN "Email"
        WHEN first_form_source = 'hs_automation' THEN "Email"
    END AS first_form_source_name,
    CASE
        WHEN first_form_source IN ('partner','partenariat') THEN "Partenariat"
        WHEN first_form_source IN ('sponsorship','parrainage') THEN "Parrainage"
        WHEN first_form_partner_referral_id IS NOT NULL THEN "Parrainage"
        WHEN (first_form_source LIKE '%adwords%') OR (first_form_source IN ('google','bing') AND first_form_medium IN ('ads','cpc','ppc','retargeting','cpm','paid')) THEN "Paid Search"
        WHEN first_form_source = 'google' AND first_form_medium = 'organic' THEN "SEO"
        WHEN first_form_source = 'youtube' AND first_form_medium LIKE '%paid%' THEN "Paid Social"
        WHEN first_form_source = 'youtube' THEN "Organic Social"
        WHEN first_form_source IN ('linkedin','tiktok','facebook','meta','instagram') AND (first_form_medium IN ('ads','cpc','ppc','retargeting','cpm') OR first_form_medium LIKE '%paid%') THEN "Paid Social"
        WHEN first_form_source LIKE '%paid%' AND first_form_medium LIKE '%facebook%' THEN "Paid Social"
        WHEN first_form_source LIKE '%facebook%' AND first_form_medium LIKE '%ads%' THEN "Paid Social"
        WHEN first_form_source LIKE '%linkedin%' AND first_form_medium = 'organic' THEN "Organic Social"
        WHEN first_form_source LIKE '%tiktok%' AND first_form_medium IN ('organic','social') THEN "Organic Social"
        WHEN first_form_source IN ('facebook','instagram','ig') AND first_form_medium = 'social' THEN "Organic Social"        
        WHEN first_form_source = 'bing' AND first_form_medium = 'organic' THEN "SEO"
        WHEN first_form_source = 'affiliation' THEN "Affiliation"
        WHEN first_form_source LIKE '%adyen%' OR first_form_source LIKE '%ogone%' OR first_form_source LIKE '%getalma%' OR first_form_source LIKE '%klarna%' THEN "Payment Service Provider"
        WHEN first_form_source = 'onsite' THEN "Onsite banner"
        WHEN first_form_source = 'gmb' THEN "GMB"
        WHEN first_form_source LIKE '%hillspet%' THEN "Criteo"
        WHEN first_form_source LIKE '%qwant%' AND first_form_medium = 'referral' THEN "SEO"
        WHEN first_form_source LIKE '%bing%' AND first_form_medium = 'referral' THEN "SEO"
        WHEN first_form_source LIKE '%google%' AND first_form_medium = 'referral' THEN "SEO"
        WHEN first_form_source LIKE '%yahoo%' AND first_form_medium = 'referral' THEN "SEO"
        WHEN first_form_source LIKE '%yahoo%' AND first_form_medium = 'organic' THEN "SEO"
        WHEN first_form_source LIKE '%duckduckgo%' AND first_form_medium = 'referral' THEN "SEO"
        WHEN first_form_source LIKE '%duckduckgo%' AND first_form_medium = 'organic' THEN "SEO"
        WHEN first_form_source LIKE '%ecosia%' AND first_form_medium = 'referral' THEN "SEO"
        WHEN first_form_source LIKE '%ecosia%' AND first_form_medium = 'organic' THEN "SEO"
        WHEN first_form_source LIKE '%baidu%' AND first_form_medium = 'organic' THEN "SEO"
        WHEN first_form_source LIKE '%brave%' AND first_form_medium = 'referral' THEN "SEO"
        WHEN first_form_medium = 'organic' THEN "SEO"
        WHEN first_form_source LIKE '%twitch%' AND first_form_medium = 'social' THEN "Organic Social"
        WHEN first_form_source LIKE '%twitter%' AND first_form_medium = 'social' THEN "Organic Social"
        WHEN first_form_source LIKE '%reddit%' AND first_form_medium = 'social' THEN "Organic Social"
        WHEN first_form_source LIKE '%tiktok%' AND first_form_medium = 'referral' THEN "Organic Social"
        WHEN first_form_source LIKE '%meta%' AND first_form_medium = 'referral' THEN "Organic Social"
        WHEN first_form_source LIKE '%facebook%' AND first_form_medium = 'referral' THEN "Organic Social"
        WHEN first_form_source LIKE '%instagram%' AND first_form_medium = 'referral' THEN "Organic Social"
        WHEN first_form_source LIKE '%snapchat%' AND first_form_medium = 'referral' THEN "Organic Social"
        WHEN first_form_source LIKE '%pinterest%' AND first_form_medium = 'referral' THEN "Organic Social"
        WHEN first_form_source LIKE '%twitter%' AND first_form_medium = 'referral' THEN "Organic Social"
        WHEN first_form_source LIKE '%linkedin%' AND first_form_medium = 'referral' THEN "Organic Social"
        WHEN first_form_source LIKE '%chatgpt%' THEN "AI Referrals"
        WHEN first_form_source LIKE '%perplexity%' THEN "AI Referrals"
        WHEN first_form_medium LIKE '%referral%' THEN "Referral Websites"        
        WHEN first_form_medium LIKE '%webinar%' THEN "Webinar"
        WHEN first_form_source = 'direct' AND first_form_medium = 'none' THEN "Direct"
        WHEN first_form_source = '(direct)' AND first_form_medium = '(none)' THEN "Direct"
        WHEN first_form_source LIKE '%mail%' THEN "Email"
        WHEN first_form_medium LIKE '%mail%' THEN "Email"
        WHEN first_form_source = 'hs_automation' THEN "Email"
    END AS first_form_channel,    
    first_form_campaign,
    date_lost,
    date_lead,
    date_mql,
    date_opportunity,
    date_won_invoicing,
    date_won_creation,
    date_won_accounting,
    subscription_plan,
    pack_choice
  FROM
    {{ ref('fct_contact_stages_attributions') }}
),
final_layer AS (
  SELECT
    c.*,
    CAST(NULL AS STRING) AS event_type,
    CAST(NULL AS DATE) AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,
    CAST(NULL AS FLOAT64) AS prestation_amount
  FROM
    contact_stages c
  
  UNION ALL

  SELECT
    c.*,
    "prestation" AS event_type,
    date AS event_date,
    prestation_name_standard AS prestation_name,
    prestation_category,
    data_prestation_category,    
    ROUND(prestation_amount,2) AS prestation_amount
  FROM
    paid_prestations p
    LEFT JOIN
      contact_stages c
    ON
      c.dougs_company_id = p.company_id

  UNION ALL

  SELECT
    c.*,
    "lost" AS event_type,
    date_lost AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,    
    CAST(NULL AS FLOAT64) AS prestation_amount
  FROM
    contact_stages c
  WHERE
    date_lost IS NOT NULL      

  UNION ALL

  SELECT
    c.*,
    "lead" AS event_type,
    date_lead AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,    
    CAST(NULL AS FLOAT64) AS prestation_amount
  FROM
    contact_stages c
  WHERE
    date_lead IS NOT NULL

  UNION ALL

  SELECT
    c.*,
    "mql" AS event_type,
    date_mql AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,    
    CAST(NULL AS FLOAT64) AS prestation_amount
  FROM
    contact_stages c
  WHERE
    date_mql IS NOT NULL    

  UNION ALL

  SELECT
    c.*,
    "opportunity" AS event_type,
    date_opportunity AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,    
    CAST(NULL AS FLOAT64) AS prestation_amount
  FROM
    contact_stages c
  WHERE
    date_opportunity IS NOT NULL      

  UNION ALL

  SELECT
    c.*,
    "won_invoicing" AS event_type,
    date_won_invoicing AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,    
    CAST(NULL AS FLOAT64) AS prestation_amount
  FROM
    contact_stages c
  WHERE
    date_won_invoicing IS NOT NULL

  UNION ALL

  SELECT
    c.*,
    "won_creation" AS event_type,
    date_won_creation AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,    
    CAST(NULL AS FLOAT64) AS prestation_amount
  FROM
    contact_stages c
  WHERE
    date_won_creation IS NOT NULL   

  UNION ALL

  SELECT
    c.*,
    "won_accounting" AS event_type,
    date_won_accounting AS event_date,
    CAST(NULL AS STRING) AS prestation_name,
    CAST(NULL AS STRING) AS prestation_category,
    CAST(NULL AS STRING) AS data_prestation_category,    
    CAST(NULL AS FLOAT64) AS prestation_amount
  FROM
    contact_stages c
  WHERE
    date_won_accounting IS NOT NULL           
)
SELECT
  *
FROM
  final_layer
