SELECT
  ps.ps_partkey,
  sum(ps.ps_supplycost * ps.ps_availqty) AS value
FROM
    [partsupp] AS ps,
  	[supplier] AS s,
  	[nation] AS n
WHERE
  ps.ps_suppkey = s.s_suppkey
  AND s.s_nationkey = n.n_nationkey
  AND n.n_name = 'JAPAN'
GROUP BY
  ps.ps_partkey HAVING
    sum(ps.ps_supplycost * ps.ps_availqty) > (
      SELECT
        sum(ps.ps_supplycost * ps.ps_availqty) * 0.0001000000
      FROM
        [partsupp] AS ps,
        [supplier] AS s,
        [nation] AS n
      WHERE
        ps.ps_suppkey = s.s_suppkey
        AND s.s_nationkey = n.n_nationkey
        AND n.n_name = 'JAPAN'
    )
ORDER BY
  value DESC;
