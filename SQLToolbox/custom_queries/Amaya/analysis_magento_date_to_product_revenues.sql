SELECT  
	*,
    "current" as date_range
FROM
	`amasty-data.analysis.analysis_magento_date_to_product`
WHERE
	date between PARSE_DATE('%Y%m%d',  @DS_START_DATE) AND PARSE_DATE('%Y%m%d',  @DS_END_DATE)
	and
    item_total_revenue_ttc != 0

UNION ALL

SELECT  
	*,
    "previous_year" as date_range
FROM
	`amasty-data.analysis.analysis_magento_date_to_product`
WHERE
	date between date_sub(PARSE_DATE('%Y%m%d',  @DS_START_DATE),INTERVAL 1 YEAR) AND date_sub(PARSE_DATE('%Y%m%d',  @DS_END_DATE),INTERVAL 1 YEAR)
	and
    item_total_revenue_ttc != 0
