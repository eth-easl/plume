SELECT count(*)
FROM
  (SELECT DISTINCT c_last_name,
                   c_first_name,
                   d_date
   FROM [store_sales] AS store_sales,
        [date_dim] AS date_dim,
        [customer] AS customer
   WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
     AND store_sales.ss_customer_sk = customer.c_customer_sk
     AND d_month_seq BETWEEN 1200 AND 1200 + 11 INTERSECT
     SELECT DISTINCT c_last_name,
                     c_first_name,
                     d_date
     FROM [catalog_sales] AS catalog_sales,
          [date_dim] AS date_dim,
          [customer] AS customer WHERE catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
     AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
     AND d_month_seq BETWEEN 1200 AND 1200 + 11 INTERSECT
     SELECT DISTINCT c_last_name,
                     c_first_name,
                     d_date
     FROM [web_sales] AS web_sales,
          [date_dim] AS date_dim,
          [customer] AS customer WHERE web_sales.ws_sold_date_sk = date_dim.d_date_sk
     AND web_sales.ws_bill_customer_sk = customer.c_customer_sk
     AND d_month_seq BETWEEN 1200 AND 1200 + 11 ) hot_cust
LIMIT 100;

