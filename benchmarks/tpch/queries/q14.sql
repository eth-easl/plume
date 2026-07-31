SELECT
    100.00 * sum(
        CASE WHEN p_type LIKE 'PROMO%' THEN
            l_extendedprice * (1 - l_discount)
        ELSE
            0
        END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM
    [lineitem] AS l,
	[part] AS p
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= '1995-09-01'
    AND l_shipdate < '1995-10-01';

