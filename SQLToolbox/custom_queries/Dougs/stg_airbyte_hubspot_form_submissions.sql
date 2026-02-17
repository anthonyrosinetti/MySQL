WITH form_submissions_with_values AS (
    -- Transforming the JSON ARRAY into a REPEATED JSON in order to be able to unnest each value submitted in the form
    SELECT
        conversionId AS form_submission_id,
        formId AS form_id,
        submittedAt AS submission_timestamp,
        pageUrl AS page_url,
        JSON_QUERY_ARRAY(values) AS formatted_values
    FROM
        {{ source('bronze_airbyte_hubspot', 'form_submissions') }}
)

-- Unnesting all form submission values, and then using FIRST_VALUE IGNORE NULLS to keep only one row per form submission
SELECT DISTINCT
    form_submission_id,
    form_id,
    submission_timestamp,
    page_url,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "email" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS email_address,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "product_type" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS product_type,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "utm_source" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS utm_source,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "utm_medium" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS utm_medium,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "utm_campaign" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS utm_campaign,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "utm_content" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS utm_content,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "utm_term" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS utm_term,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_utm_source" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_utm_source,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_utm_medium" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_utm_medium,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_utm_campaign" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_utm_campaign,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_utm_id" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_utm_id,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_utm_content" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_utm_content,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_utm_term" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_utm_term,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_referrer" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_referrer,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_landing_page" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_landing_page,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_gclid" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_gclid,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_fbclid" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_fbclid,
    FIRST_VALUE(CASE WHEN LAX_STRING(JSON_QUERY(val, "$.name")) = "session_ga_visitor_id" THEN LAX_STRING(JSON_QUERY(val, "$.value")) END IGNORE NULLS) OVER w AS session_ga_visitor_id,
FROM
    form_submissions_with_values,
    UNNEST(formatted_values) AS val
WHERE
    ARRAY_LENGTH(formatted_values) > 0
WINDOW
    w AS (
        PARTITION BY form_submission_id
        ORDER BY form_submission_id -- We don't really care as there is only 1 value per form submission for column
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )

UNION ALL

-- In case form submission does not contain any value, adding empty form submission here as the above UNNEST would have removed it
SELECT
    form_submission_id,
    form_id,
    submission_timestamp,
    page_url,
    CAST(NULL AS STRING) AS email_address,
    CAST(NULL AS STRING) AS product_type,
    CAST(NULL AS STRING) AS utm_source,
    CAST(NULL AS STRING) AS utm_medium,
    CAST(NULL AS STRING) AS utm_campaign,
    CAST(NULL AS STRING) AS utm_content,
    CAST(NULL AS STRING) AS utm_term,
    CAST(NULL AS STRING) AS session_utm_source,
    CAST(NULL AS STRING) AS session_utm_medium,
    CAST(NULL AS STRING) AS session_utm_campaign,
    CAST(NULL AS STRING) AS session_utm_id,
    CAST(NULL AS STRING) AS session_utm_content,
    CAST(NULL AS STRING) AS session_utm_term,
    CAST(NULL AS STRING) AS session_referrer,
    CAST(NULL AS STRING) AS session_landing_page,
    CAST(NULL AS STRING) AS session_gclid,
    CAST(NULL AS STRING) AS session_fbclid,
    CAST(NULL AS STRING) AS session_ga_visitor_id,
FROM
    form_submissions_with_values
WHERE
    ARRAY_LENGTH(formatted_values) = 0
