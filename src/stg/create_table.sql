CREATE TABLE stg.deliveries (
	order_id 		text primary key,
	order_ts 		timestamp,
	delivery_id 	text,
	courier_id		text,
	address			text,
	delivery_ts     timestamp,
	rate 			int,
	sum 			numeric(14, 2),
	tip_sum 		numeric(14, 2)
);
CREATE TABLE stg.couriers (
	id   text primary key,
	name text
);
CREATE TABLE stg.restaurants (
	id 	 text primary key,
	name text
);