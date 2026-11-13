WITH ss AS
  ( SELECT i_manufact_id,
           sum(ss_ext_sales_price) total_sales
   FROM [store_sales] AS store_sales,
        [date_dim] AS date_dim,
        [customer_address] AS customer_address,
        [item] AS item
   WHERE i_manufact_id IN
       (SELECT i_manufact_id
        FROM [item] AS item
        WHERE i_category IN ('Electronics'))
     AND ss_item_sk = i_item_sk
     AND ss_sold_date_sk = d_date_sk
     AND d_year = 1998
     AND d_moy = 5
     AND ss_addr_sk = ca_address_sk
     AND ca_gmt_offset = -5
   GROUP BY i_manufact_id),
     cs AS
  ( SELECT i_manufact_id,
           sum(cs_ext_sales_price) total_sales
   FROM [catalog_sales] AS catalog_sales,
        [date_dim] AS date_dim,
        [customer_address] AS customer_address,
        [item] AS item
   WHERE i_manufact_id IN
       (SELECT i_manufact_id
        FROM [item] AS item
        WHERE i_category IN ('Electronics'))
     AND cs_item_sk = i_item_sk
     AND cs_sold_date_sk = d_date_sk
     AND d_year = 1998
     AND d_moy = 5
     AND cs_bill_addr_sk = ca_address_sk
     AND ca_gmt_offset = -5
   GROUP BY i_manufact_id),
     ws AS
  ( SELECT i_manufact_id,
           sum(ws_ext_sales_price) total_sales
   FROM [web_sales] AS web_sales,
        [date_dim] AS date_dim,
        [customer_address] AS customer_address,
        [item] AS item
   WHERE i_manufact_id IN
       (SELECT i_manufact_id
        FROM [item] AS item
        WHERE i_category IN ('Electronics'))
     AND ws_item_sk = i_item_sk
     AND ws_sold_date_sk = d_date_sk
     AND d_year = 1998
     AND d_moy = 5
     AND ws_bill_addr_sk = ca_address_sk
     AND ca_gmt_offset = -5
   GROUP BY i_manufact_id)
SELECT i_manufact_id,
       sum(total_sales) total_sales
FROM
  (SELECT *
   FROM ss
   UNION ALL SELECT *
   FROM cs
   UNION ALL SELECT *
   FROM ws) tmp1
GROUP BY i_manufact_id
ORDER BY total_sales
LIMIT 100;

