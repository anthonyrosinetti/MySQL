WITH lead_linear_contacts AS (
    SELECT
        contact_id,
        l.touchpoint_timestamp AS lead_linear_touchpoint_timestamp,
        CASE
            WHEN l.touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN l.partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_lead_linear.source_name) OVER (w_lead_linear_source_medium),"Unmapped")
        END AS lead_linear_source_name,
        CASE
            WHEN l.touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN l.partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_lead_linear.channel) OVER (w_lead_linear_source_medium),"Unmapped")
        END AS lead_linear_channel,
        CASE
            WHEN l.touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN l.partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(camp_lead_linear.campaign_group) OVER (w_lead_linear_campaign),"Unmapped")
        END AS lead_linear_campaign_group,
        IFNULL(l.utm_term,"No keyword") AS lead_linear_utm_term,
        l.weight AS lead_linear_weight,
    FROM
        {{ ref('fct_contact_stages_attribution') }} c,UNNEST(lead_linear_touchpoint) l
    LEFT JOIN
        {{ ref('dim_mapping_source_medium') }} s_lead_linear
    ON
        (s_lead_linear.utm_source IS NULL OR LOWER(l.utm_source) LIKE LOWER(s_lead_linear.utm_source))
        AND
        (s_lead_linear.utm_medium IS NULL OR LOWER(l.utm_medium) LIKE LOWER(s_lead_linear.utm_medium)) 
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} camp_lead_linear
    ON
        (camp_lead_linear.utm_campaign IS NULL OR LOWER(l.utm_campaign) LIKE LOWER(camp_lead_linear.utm_campaign))
        AND
        (camp_lead_linear.utm_source IS NULL OR LOWER(l.utm_source) LIKE LOWER(camp_lead_linear.utm_source))
        AND
        (camp_lead_linear.utm_medium IS NULL OR LOWER(l.utm_medium) LIKE LOWER(camp_lead_linear.utm_medium))
    WINDOW w_lead_linear_source_medium AS (
        PARTITION BY s_lead_linear.utm_source, s_lead_linear.utm_medium
        ORDER BY s_lead_linear.sort ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ),  
    w_lead_linear_campaign AS (
        PARTITION BY camp_lead_linear.utm_campaign, camp_lead_linear.utm_source, camp_lead_linear.utm_medium
        ORDER BY camp_lead_linear.sort ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )       
),
aggregated_lead_linear_contacts AS (
    SELECT DISTINCT
        contact_id,
        ARRAY_AGG(
            STRUCT(
                lead_linear_touchpoint_timestamp,
                lead_linear_source_name,
                lead_linear_channel,
                lead_linear_campaign_group,
                lead_linear_utm_term,
                lead_linear_weight
            )
        ) AS lead_linear
    FROM
        lead_linear_contacts
    WHERE
        contact_id IS NOT NULL
    GROUP BY
        contact_id
),
mql_linear_contacts AS (
    SELECT
        contact_id,
        m.touchpoint_timestamp AS mql_linear_touchpoint_timestamp,
        CASE
            WHEN m.touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN m.partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_mql_linear.source_name) OVER (w_mql_linear_source_medium),"Unmapped")
        END AS mql_linear_source_name,
        CASE
            WHEN m.touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN m.partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_mql_linear.channel) OVER (w_mql_linear_source_medium),"Unmapped")
        END AS mql_linear_channel,
        CASE
            WHEN m.touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN m.partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(camp_mql_linear.campaign_group) OVER (w_mql_linear_campaign),"Unmapped")
        END AS mql_linear_campaign_group,
        IFNULL(m.utm_term,"No keyword") AS mql_linear_utm_term,
        m.weight AS mql_linear_weight,
    FROM
        {{ ref('fct_contact_stages_attribution') }} c,UNNEST(mql_linear_touchpoint) m
    LEFT JOIN
        {{ ref('dim_mapping_source_medium') }} s_mql_linear
    ON
        (s_mql_linear.utm_source IS NULL OR LOWER(m.utm_source) LIKE LOWER(s_mql_linear.utm_source))
        AND
        (s_mql_linear.utm_medium IS NULL OR LOWER(m.utm_medium) LIKE LOWER(s_mql_linear.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} camp_mql_linear
    ON
        (camp_mql_linear.utm_campaign IS NULL OR LOWER(m.utm_campaign) LIKE LOWER(camp_mql_linear.utm_campaign))
        AND
        (camp_mql_linear.utm_source IS NULL OR LOWER(m.utm_source) LIKE LOWER(camp_mql_linear.utm_source))
        AND
        (camp_mql_linear.utm_medium IS NULL OR LOWER(m.utm_medium) LIKE LOWER(camp_mql_linear.utm_medium))
    WINDOW w_mql_linear_source_medium AS (
        PARTITION BY s_mql_linear.utm_source, s_mql_linear.utm_medium
        ORDER BY s_mql_linear.sort ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ),
    w_mql_linear_campaign AS (
        PARTITION BY camp_mql_linear.utm_campaign, camp_mql_linear.utm_source, camp_mql_linear.utm_medium
        ORDER BY camp_mql_linear.sort ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )        
),
aggregated_mql_linear_contacts AS (
    SELECT DISTINCT
        contact_id,
        ARRAY_AGG(
            STRUCT(
                mql_linear_touchpoint_timestamp,
                mql_linear_source_name,
                mql_linear_channel,
                mql_linear_campaign_group,
                mql_linear_utm_term,
                mql_linear_weight
            )
        ) AS mql_linear
    FROM
        mql_linear_contacts
    WHERE
        contact_id IS NOT NULL
    GROUP BY
        contact_id
),
final_layer AS (
    SELECT DISTINCT
        contact_id,
        dougs_user_id,
        company_id,
        dougs_company_id,
        gender,
        age_range,
        eligible,
        legal_form,
        country,
        ape_activity_name,
        activity,
        first_conversion_form,
        first_conversion_form_category,
        first_conversion_form_type,
        IFNULL(contact_first_page,"-") AS first_page,
        lead_source,
        original_source,
        contact_category,
        lost_reason,
        contact_type,
        treatment_type,
        first_touchpoint_timestamp,
        CASE
            WHEN first_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_touchpoint.source_name) OVER (w_touchpoint_source_medium),"Unmapped")
        END AS first_touchpoint_source_name,
        CASE
            WHEN first_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_touchpoint.channel) OVER (w_touchpoint_source_medium),"Unmapped")
        END AS first_touchpoint_channel,
        CASE
            WHEN first_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(camp_touchpoint.campaign_group) OVER (w_touchpoint_campaign),"Unmapped")
        END AS first_touchpoint_campaign_group,
        IFNULL(first_touchpoint_term,"No keyword") AS first_touchpoint_term,
        1 AS first_touchpoint_weight,
        first_form_timestamp,
        CASE
            WHEN first_form_timestamp IS NULL THEN "No touchpoint"
            WHEN first_form_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_form.source_name) OVER (w_form_source_medium),"Unmapped")
        END AS first_form_source_name,
        CASE
            WHEN first_form_timestamp IS NULL THEN "No touchpoint"
            WHEN first_form_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_form.channel) OVER (w_form_source_medium),"Unmapped")
        END AS first_form_channel,        
        CASE
            WHEN first_form_timestamp IS NULL THEN "No touchpoint"
            WHEN first_form_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(camp_form.campaign_group) OVER (w_form_campaign),"Unmapped")
        END AS first_form_campaign_group,
        IFNULL(first_form_term,"No keyword") AS first_form_term,
        1 AS first_form_weight,
        lead_last_touchpoint_timestamp,
        CASE
            WHEN lead_last_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN lead_last_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_lead.source_name) OVER (w_lead_source_medium),"Unmapped")
        END AS lead_last_touchpoint_source_name,
        CASE
            WHEN lead_last_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN lead_last_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_lead.channel) OVER (w_lead_source_medium),"Unmapped")
        END AS lead_last_touchpoint_channel,        
        CASE
            WHEN lead_last_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN lead_last_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(camp_lead.campaign_group) OVER (w_lead_campaign),"Unmapped")
        END AS lead_last_touchpoint_campaign_group,
        IFNULL(lead_last_touchpoint_term,"No keyword") AS lead_last_touchpoint_term,
        1 AS lead_last_touchpoint_weight,
        mql_last_touchpoint_timestamp,
        CASE
            WHEN mql_last_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN mql_last_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_mql.source_name) OVER (w_mql_source_medium),"Unmapped")
        END AS mql_last_touchpoint_source_name,
        CASE
            WHEN mql_last_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN mql_last_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_mql.channel) OVER (w_mql_source_medium),"Unmapped")
        END AS mql_last_touchpoint_channel,        
        CASE
            WHEN mql_last_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN mql_last_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(camp_mql.campaign_group) OVER (w_mql_campaign),"Unmapped")
        END AS mql_last_touchpoint_campaign_group,
        IFNULL(mql_last_touchpoint_term,"No keyword") AS mql_last_touchpoint_term,
        1 AS mql_last_touchpoint_weight,
        CASE
            WHEN first_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_touchpoint.source_name) OVER (w_touchpoint_source_medium),"Unmapped")
        END AS source_name,
        CASE
            WHEN first_touchpoint_timestamp IS NULL THEN "No touchpoint"
            WHEN first_touchpoint_partner_referral_id IS NOT NULL THEN "Parrainage"
            ELSE IFNULL(FIRST_VALUE(s_touchpoint.channel) OVER (w_touchpoint_source_medium),"Unmapped")
        END AS channel,        
        lead_linear,
        mql_linear,
        pre_conversion_touchpoints,
        pre_conversion_forms,        
        date_lost,
        date_lead,
        date_mql,
        date_sql,
        date_opportunity,
        date_signup_invoicing,
        date_won_invoicing,
        date_won_creation,
        date_won_accounting,
        pack_choice
    FROM
        {{ ref('fct_contact_stages_attribution') }} c
    LEFT JOIN
        aggregated_mql_linear_contacts
    USING
        (contact_id)
    LEFT JOIN
        aggregated_lead_linear_contacts
    USING
        (contact_id)        
    LEFT JOIN
        {{ ref('dim_mapping_source_medium') }} s_touchpoint
    ON
        (s_touchpoint.utm_source IS NULL OR LOWER(c.first_touchpoint_source) LIKE LOWER(s_touchpoint.utm_source))
        AND
        (s_touchpoint.utm_medium IS NULL OR LOWER(c.first_touchpoint_medium) LIKE LOWER(s_touchpoint.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_source_medium') }} s_form
    ON
        (s_form.utm_source IS NULL OR LOWER(c.first_form_source) LIKE LOWER(s_form.utm_source))
        AND
        (s_form.utm_medium IS NULL OR LOWER(c.first_form_medium) LIKE LOWER(s_form.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_source_medium') }} s_lead
    ON
        (s_lead.utm_source IS NULL OR LOWER(c.lead_last_touchpoint_source) LIKE LOWER(s_lead.utm_source))
        AND
        (s_lead.utm_medium IS NULL OR LOWER(c.lead_last_touchpoint_medium) LIKE LOWER(s_lead.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_source_medium') }} s_mql
    ON
        (s_mql.utm_source IS NULL OR LOWER(c.mql_last_touchpoint_source) LIKE LOWER(s_mql.utm_source))
        AND
        (s_mql.utm_medium IS NULL OR LOWER(c.mql_last_touchpoint_medium) LIKE LOWER(s_mql.utm_medium))      
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} camp_touchpoint
    ON
        (camp_touchpoint.utm_campaign IS NULL OR LOWER(c.first_touchpoint_campaign) LIKE LOWER(camp_touchpoint.utm_campaign))
        AND
        (camp_touchpoint.utm_source IS NULL OR LOWER(c.first_touchpoint_source) LIKE LOWER(camp_touchpoint.utm_source))
        AND
        (camp_touchpoint.utm_medium IS NULL OR LOWER(c.first_touchpoint_medium) LIKE LOWER(camp_touchpoint.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} camp_form
    ON
        (camp_form.utm_campaign IS NULL OR LOWER(c.first_touchpoint_campaign) LIKE LOWER(camp_form.utm_campaign))
        AND
        (camp_form.utm_source IS NULL OR LOWER(c.first_touchpoint_source) LIKE LOWER(camp_form.utm_source))
        AND
        (camp_form.utm_medium IS NULL OR LOWER(c.first_touchpoint_medium) LIKE LOWER(camp_form.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} camp_lead
    ON
        (camp_lead.utm_campaign IS NULL OR LOWER(c.lead_last_touchpoint_campaign) LIKE LOWER(camp_lead.utm_campaign))
        AND
        (camp_lead.utm_source IS NULL OR LOWER(c.lead_last_touchpoint_source) LIKE LOWER(camp_lead.utm_source))
        AND
        (camp_lead.utm_medium IS NULL OR LOWER(c.lead_last_touchpoint_medium) LIKE LOWER(camp_lead.utm_medium))
    LEFT JOIN
        {{ ref('dim_mapping_campaign') }} camp_mql
    ON
        (camp_mql.utm_campaign IS NULL OR LOWER(c.mql_last_touchpoint_campaign) LIKE LOWER(camp_mql.utm_campaign))
        AND
        (camp_mql.utm_source IS NULL OR LOWER(c.mql_last_touchpoint_source) LIKE LOWER(camp_mql.utm_source))
        AND
        (camp_mql.utm_medium IS NULL OR LOWER(c.mql_last_touchpoint_medium) LIKE LOWER(camp_mql.utm_medium))         
    WINDOW
        w_touchpoint_source_medium AS (
            PARTITION BY s_touchpoint.utm_source, s_touchpoint.utm_medium
            ORDER BY s_touchpoint.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_form_source_medium AS (
            PARTITION BY s_form.utm_source, s_form.utm_medium
            ORDER BY s_form.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_lead_source_medium AS (
            PARTITION BY s_lead.utm_source, s_lead.utm_medium
            ORDER BY s_lead.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_mql_source_medium AS (
            PARTITION BY s_mql.utm_source, s_mql.utm_medium
            ORDER BY s_mql.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_touchpoint_campaign AS (
            PARTITION BY camp_touchpoint.utm_campaign, camp_touchpoint.utm_source, camp_touchpoint.utm_medium
            ORDER BY camp_touchpoint.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_form_campaign AS (
            PARTITION BY camp_form.utm_campaign, camp_form.utm_source, camp_form.utm_medium
            ORDER BY camp_form.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_lead_campaign AS (
            PARTITION BY camp_lead.utm_campaign, camp_lead.utm_source, camp_lead.utm_medium
            ORDER BY camp_lead.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ),
        w_mql_campaign AS (
            PARTITION BY camp_mql.utm_campaign, camp_mql.utm_source, camp_mql.utm_medium
            ORDER BY camp_mql.sort ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )    
)
SELECT
    *,
    CAST(NULL AS STRING) AS model_type
FROM
    final_layer
