--DROP VIEW IF EXISTS de.analysis.orders;

--orders UPD
CREATE VIEW de.analysis.orders AS
SELECT o.user_id
	, o.order_id
	, o.order_ts
	, o.cost
	, os.status
FROM de.production.orders AS o
LEFT JOIN (
	SELECT order_id
		, status_id AS status
	FROM (
		SELECT order_id
			, status_id
			, ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY dttm DESC) AS row_num
		FROM de.production.orderstatuslog
		) AS t1
	WHERE row_num = 1
) AS os
	ON o.order_id=os.order_id
WHERE order_ts >= '2021-01-01 00:00:00';