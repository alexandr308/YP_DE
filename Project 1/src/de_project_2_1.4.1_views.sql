--users
CREATE VIEW de.analysis.users AS
SELECT *
FROM de.production.users;

--orders
CREATE VIEW de.analysis.orders AS
SELECT *
FROM de.production.orders
WHERE order_ts >= '2021-01-01 00:00:00'
	--AND status = 4
;

--DROP VIEW IF EXISTS de.analysis.orders;

--products
CREATE VIEW de.analysis.products AS
SELECT *
FROM de.production.products;

--orderitems
CREATE VIEW de.analysis.orderitems AS
SELECT *
FROM de.production.orderitems;

--orderstatuses
CREATE VIEW de.analysis.orderstatuses AS
SELECT *
FROM de.production.orderstatuses;