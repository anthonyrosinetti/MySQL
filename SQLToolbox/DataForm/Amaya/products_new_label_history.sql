
-- Config block
config {
  type: "incremental",
  protected: true,
  bigquery: {
    partitionBy: "TIMESTAMP_TRUNC(_ingestion_timestamp, DAY)"
  },  
  schema: "analysis"
}

-- SQL
WITH raw_data AS (
    SELECT DISTINCT
        product_id,
        LAST_VALUE(is_new_label) OVER w AS is_new_label
    FROM
        ${self()}
    WINDOW
        w AS (
            PARTITION BY product_id
            ORDER BY _ingestion_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
),

synchro AS (
    SELECT
        id AS product_id,
        CASE (
          SELECT
              json_extract_scalar(c, '$.value')
          FROM
              UNNEST(json_extract_array(custom_attributes)) c
          WHERE
              json_extract_scalar(c, '$.attribute_code') = 'label_is_new'
        )
            WHEN '1' THEN true
            WHEN '0' THEN false
        END AS is_new_label
    FROM
        ${ref("raw_magento_products")}
)

SELECT
    synchro.*,
    CURRENT_TIMESTAMP() AS _ingestion_timestamp
FROM
    synchro
LEFT JOIN
    raw_data
USING
    (product_id)
WHERE
    IFNULL(CAST(synchro.is_new_label AS STRING), "") != IFNULL(CAST(raw_data.is_new_label AS STRING), "")
