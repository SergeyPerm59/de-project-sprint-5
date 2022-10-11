CREATE TABLE dds.time (
	id				serial primary key,
	order_ts		timestamp,
	delivery_ts 	timestamp,
	year			int CHECK (month > 0 and month < 13)
);	
	
CREATE TABLE dds.courier (
	id		    serial primary key,
	courier_id  text,
	name 		text,
	rate int
);

CREATE TABLE dds.restaurants (
	id				serial primary key,
	restaurants_id  text,
	name 			text
);

CREATE TABLE dds.orders (
	id				serial primary key,
	orders_id		text unique,
	courier_id		int,
	ts_id			int,
	delivery_id		text,
	restaurants_id	int,
	address			text,
	sum				numeric(14, 2),
	tip_sum			numeric(14, 2),
	
	foreign key (ts_id) references dds.time (id),
	foreign key (courier_id) references dds.couriers (id),
	foreign key (restaurants_id) references dds.restaurants (id)
);