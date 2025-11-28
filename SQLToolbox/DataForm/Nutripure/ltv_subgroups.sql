CREATE OR REPLACE TABLE `cdp-boryl.ltv_analysis.ltv_subgroups` AS (
  SELECT
    *,
    NTILE(4) OVER (ORDER BY aov ASC) AS aov_group,
    NTILE(4) OVER (ORDER BY orders_total ASC) AS orders_total_group
  FROM
    `cdp-boryl.ltv_analysis.ltv_groups`
  WHERE
    cltv_group = 1

  UNION ALL

    SELECT
    *,
    NTILE(4) OVER (ORDER BY aov ASC) AS aov_group,
    NTILE(4) OVER (ORDER BY orders_total ASC) AS orders_total_group
  FROM
    `cdp-boryl.ltv_analysis.ltv_groups`
  WHERE
    cltv_group = 2

  UNION ALL

    SELECT
    *,
    NTILE(4) OVER (ORDER BY aov ASC) AS aov_group,
    NTILE(4) OVER (ORDER BY orders_total ASC) AS orders_total_group
  FROM
    `cdp-boryl.ltv_analysis.ltv_groups`
  WHERE
    cltv_group = 3

  UNION ALL

    SELECT
    *,
    NTILE(4) OVER (ORDER BY aov ASC) AS aov_group,
    NTILE(4) OVER (ORDER BY orders_total ASC) AS orders_total_group
  FROM
    `cdp-boryl.ltv_analysis.ltv_groups`
  WHERE
    cltv_group = 4
)
