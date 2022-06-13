CREATE OR REPLACE VIEW shipping_datamart AS
SELECT si.shippingid
	, si.vendorid
	, st.transfer_type
	, DATE_PART('day', ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime) AS full_day_at_shipping
	, CASE WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN 1 ELSE 0 END AS is_delay
	, CASE WHEN ss.status = 'finished' THEN 1 ELSE 0 END AS is_shipping_finish
	, CASE WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
			THEN DATE_PART('day', ss.shipping_end_fact_datetime - si.shipping_plan_datetime)
			ELSE 0
	END AS delay_day_at_shipping
	, payment_amount
	, payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) AS vat
	, payment_amount * sa.agreement_commission AS profit
FROM public.shipping_info AS si
JOIN public.shipping_transfer AS st 
	ON si.transfer_type_id=st.transfer_type_id
JOIN public.shipping_status AS ss
	ON si.shippingid=ss.shippingid
JOIN public.shipping_country_rates AS scr
	ON scr.shipping_country_id=si.shipping_country_id
JOIN public.shipping_agreement AS sa
	ON sa.agreementid=si.agreementid
;
