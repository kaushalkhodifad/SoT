SELECT
	s.shipment_id,
	order_id,
	shipment_status,
	TYPE,
	carrier,
	shipment_created_at,
	pending_ts,
	rts_ts,
	pur_ts,
	intransit_ts,
	ofd_ts,
	delivered_ts,
	rto_ts,
	rto_ofd_ts,
	undelivered_ts,
	shipment_is_deleted,
	shipment_is_active,
	awb,
	vendor_id,
	total_cost,
	delivery_cost,
	tax,
	service_type,
	full_service_name,
	forward_shipment_id,
	carrier_service,
	pickup_attempts,
	collection_amount,
	is_external_shipment,
	rate_id,
	cod_charge,
	gst_charge,
	discount_inr,
	freight_charge,
	original_cod_charge,
	total_delivery_charge,
	original_freight_charge,
	estimated_delivery_time,
	estimated_pickup_time,
	third_party_order_id,
	third_party_warehouse_id,
	parent_awb,
	cancellation_reason,
	warehouse_id,
	wh_city,
	wh_name,
	wh_state,
	wh_country,
	wh_pincode,
	wh_mobile_number,
	wh_address_line_1,
	wh_address_line_2,
	wh_contact_person_name,
	is_reverse_shipment
FROM ((
		SELECT
			id AS shipment_id,
			created_at AS shipment_created_at,
			is_deleted AS shipment_is_deleted,
			is_active AS shipment_is_active,
			status AS shipment_status,
			TYPE,
			carrier,
			awb,
			order_id,
			vendor_id,
			total_cost,
			delivery_cost,
			tax,
			carrier_meta -> 'carrier_service_json' ->> 'name' AS service_type,
			carrier_meta -> 'carrier_service_json' ->> 'full_service_name' AS full_service_name,
			forward_shipment_id,
			carrier_service,
			pickup_attempts,
			collection_amount,
			is_external_shipment,
			rates_dict ->> 'Zone' AS zone,
			rates_dict ->> 'rate_id' AS rate_id,
			rates_dict ->> 'cod_charge' AS cod_charge,
			rates_dict ->> 'gst_charge' AS gst_charge,
			rates_dict ->> 'discount_inr' AS discount_inr,
			rates_dict ->> 'freight_charge' AS freight_charge,
			rates_dict ->> 'original_cod_charge' AS original_cod_charge,
			rates_dict ->> 'total_delivery_charge' AS total_delivery_charge,
			rates_dict ->> 'original_freight_charge' AS original_freight_charge,
			estimated_delivery_time,
			estimated_pickup_time,
			third_party_order_id,
			third_party_warehouse_id,
			parent_awb,
			cancellation_reason,
			warehouse_id,
			warehouse_address_snapshot ->> 'city' AS wh_city,
			warehouse_address_snapshot ->> 'name' AS wh_name,
			warehouse_address_snapshot ->> 'state' AS wh_state,
			warehouse_address_snapshot ->> 'country' AS wh_country,
			warehouse_address_snapshot ->> 'pincode' AS wh_pincode,
			warehouse_address_snapshot ->> 'mobile_number' AS wh_mobile_number,
			warehouse_address_snapshot ->> 'address_line_1' AS wh_address_line_1,
			warehouse_address_snapshot ->> 'address_line_2' AS wh_address_line_2,
			warehouse_address_snapshot ->> 'contact_person_name' AS wh_contact_person_name,
			is_reverse_shipment
		FROM
			optimus_shipment
		WHERE (created_at)::date BETWEEN date_trunc('month', CURRENT_DATE - INTERVAL '12 month')
		AND(CURRENT_DATE - INTERVAL '1 month')::date) s
	LEFT JOIN (WITH cte AS (
			SELECT
				id AS ss_id,
				shipment_id,
				CASE WHEN status = 0 THEN
					created_at
				END AS pending_ts,
				CASE WHEN status = 1 THEN
					created_at
				END AS rts_ts,
				CASE WHEN status = 2 THEN
					created_at
				END AS pur_ts,
				CASE WHEN status = 3 THEN
					created_at
				END AS intransit_ts,
				CASE WHEN status = 13 THEN
					created_at
				END AS ofd_ts,
				CASE WHEN status = 4 THEN
					created_at
				END AS delivered_ts,
				CASE WHEN status = 20 THEN
					created_at
				END AS rto_ts,
				CASE WHEN status = 21 THEN
					created_at
				END AS rto_ofd_ts,
				CASE WHEN status = 11 THEN
					created_at
				END AS undelivered_ts
			FROM
				optimus_shipmentstatus
)
		SELECT
			shipment_id,
			MAX(pending_ts) AS pending_ts,
			MAX(rts_ts) AS rts_ts,
			MAX(pur_ts) AS pur_ts,
			MAX(intransit_ts) AS intransit_ts,
			MAX(ofd_ts) AS ofd_ts,
			MAX(delivered_ts) AS delivered_ts,
			MAX(rto_ts) AS rto_ts,
			MAX(rto_ofd_ts) AS rto_ofd_ts,
			MAX(undelivered_ts) AS undelivered_ts
		FROM
			cte
		GROUP BY
			shipment_id) ss ON s.shipment_id = ss.shipment_id
);
