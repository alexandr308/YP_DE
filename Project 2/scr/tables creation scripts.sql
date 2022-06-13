DROP TABLE IF EXISTS public.shipping_country_rates;
DROP TABLE IF EXISTS public.shipping_agreement;
DROP TABLE IF EXISTS public.shipping_transfer;
DROP TABLE IF EXISTS public.shipping_info;
DROP TABLE IF EXISTS public.shipping_status;

CREATE TABLE public.shipping_country_rates (
	shipping_country_id SERIAL,
	shipping_country TEXT,
	shipping_country_base_rate NUMERIC(14,3),
	PRIMARY KEY (shipping_country_id)
);

CREATE TABLE public.shipping_agreement (
	agreementid BIGINT,
	agreement_number TEXT,
	agreement_rate NUMERIC(14,2),
	agreement_commission NUMERIC(14,3),
	PRIMARY KEY (agreementid)
);

CREATE TABLE public.shipping_transfer (
	transfer_type_id SERIAL,
	transfer_type VARCHAR(2),
	transfer_model TEXT,
	shipping_transfer_rate NUMERIC(14,3),
	PRIMARY KEY (transfer_type_id)
);

CREATE TABLE shipping_info (
	shippingid INT8,
	vendorid INT8,
	payment_amount NUMERIC(14,2),
	shipping_plan_datetime TIMESTAMP,
	transfer_type_id BIGINT,
	shipping_country_id BIGINT,
	agreementid BIGINT,
	FOREIGN KEY (transfer_type_id) REFERENCES public.shipping_transfer(transfer_type_id) ON UPDATE CASCADE,
	FOREIGN KEY (shipping_country_id) REFERENCES public.shipping_country_rates(shipping_country_id) ON UPDATE CASCADE,
	FOREIGN KEY (agreementid) REFERENCES public.shipping_agreement(agreementid)
);

CREATE TABLE public.shipping_status (
	shippingid INT8,
	status TEXT,
	state TEXT,
	shipping_start_fact_datetime TIMESTAMP,
	shipping_end_fact_datetime TIMESTAMP
)

