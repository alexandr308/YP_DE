INSERT INTO de.analysis.dm_rfm_segments
SELECT user_id
	, NTILE(5) OVER(ORDER BY MAX(order_ts) DESC NULLS FIRST) AS recenty
	, NTILE(5) OVER(ORDER BY COUNT(order_id) ASC NULLS FIRST) AS frequency
	, NTILE(5) OVER(ORDER BY SUM(cost) ASC NULLS FIRST) AS monetary_value
FROM (
	SELECT u.id AS user_id
		, o.order_id
		, o.order_ts
		, o.cost
	FROM de.analysis.users AS u
	LEFT JOIN (
		SELECT user_id
			, order_id
			, order_ts
			, cost
		FROM de.analysis.orders
		WHERE status = 4
	) AS o
		ON o.user_id=u.id
	) AS t1
GROUP BY user_id
;


SELECT COUNT(*)
FROM de.analysis.dm_rfm_segments;