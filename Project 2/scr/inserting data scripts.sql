--shipping_country_rates filling
CREATE SEQUENCE shipping_country_id_sequence START 1;
INSERT INTO public.shipping_country_rates (shipping_country_id, shipping_country, shipping_country_base_rate)
SELECT NEXTVAL('shipping_country_id_sequence') AS shipping_country_id
	, shipping_country
	, shipping_country_base_rate
FROM (
	SELECT DISTINCT shipping_country
		, shipping_country_base_rate
	FROM public.shipping
) AS s;
DROP SEQUENCE shipping_country_id_sequence;

--shipping_agreement filling
INSERT INTO public.shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)
SELECT DISTINCT vendor_description[1]::BIGINT as agreementid
	, vendor_description[2] as agreement_number
	, vendor_description[3]::NUMERIC(14,2) as agreement_rate
	, vendor_description[4]::NUMERIC(14,3) as agreement_commission
FROM (
	SELECT regexp_split_to_array(vendor_agreement_description, E':+') as vendor_description 
	FROM public.shipping
) AS t1;

--shipping_transfer filling
CREATE SEQUENCE shipping_transfer_id_sequence START 1;
INSERT INTO public.shipping_transfer (transfer_type_id, transfer_type, transfer_model, shipping_transfer_rate)
SELECT NEXTVAL('shipping_transfer_id_sequence') AS transfer_type_id
	, transfer_description[1] as transfer_type
	, transfer_description[2] as transfer_model
	, shipping_transfer_rate
FROM (
	SELECT regexp_split_to_array(shipping_transfer_description, E':+') as transfer_description
		, AVG(shipping_transfer_rate)::NUMERIC(14,3) AS shipping_transfer_rate
	FROM public.shipping
	GROUP BY 1
) AS t1
;
DROP SEQUENCE shipping_transfer_id_sequence;

--shipping_info filling
INSERT INTO public.shipping_info (shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
SELECT shippingid
	, vendorid 
	, payment_amount 
	, shipping_plan_datetime 
	, transfer_type_id
	, shipping_country_id
	, agreementid
FROM (
	SELECT distinct shippingid
		, vendorid
		, payment_amount
		, shipping_plan_datetime
		, shipping_country
		, vendor_description[1]::BIGINT as agreementid
		, transfer_description[1] as transfer_type
		, transfer_description[2] as transfer_model
	FROM (
		SELECT *
			, regexp_split_to_array(shipping_transfer_description, E':+') as transfer_description
			, regexp_split_to_array(vendor_agreement_description, E':+') as vendor_description
		FROM public.shipping
	) AS t1
) AS s
JOIN (
	SELECT transfer_type_id
		, transfer_type 
		, transfer_model
	FROM public.shipping_transfer
) AS st ON st.transfer_type=s.transfer_type AND st.transfer_model=s.transfer_model
JOIN (
	SELECT shipping_country_id
		, shipping_country
	FROM public.shipping_country_rates
) AS scr ON scr.shipping_country=s.shipping_country
;

--shipping_status filling
INSERT INTO public.shipping_status (shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
SELECT s.shippingid 
	, status
	, state
	, shipping_start_fact_datetime
	, shipping_end_fact_datetime
FROM (
	SELECT shippingid
		, MAX(state_datetime) as dt
		, MAX(CASE WHEN state = 'booked' THEN state_datetime ELSE NULL END) AS shipping_start_fact_datetime
		, MAX(CASE WHEN state = 'recieved' THEN state_datetime ELSE NULL END) AS shipping_end_fact_datetime
	FROM public.shipping
	GROUP BY 1
) AS grp
JOIN public.shipping AS s
	ON s.shippingid=grp.shippingid AND s.state_datetime=grp.dt
;
