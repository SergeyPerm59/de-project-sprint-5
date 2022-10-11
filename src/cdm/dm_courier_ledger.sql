CREATE TABLE cdm.dm_courier_ledger (
	id int NOT NULL primary key,
	courier_id int NOT NULL,
	courier_name VACHAR NOT NULL,
	settlement_year SMALLINT NOT NULL CHECK (settlement_year >= 2022 and settlement_year < 2500),
	settlement_month SMALLINT NOT NULL CHECK (settlement_month >= 1 and settlement_month < 12),
	orders_count int NOT NULL CHECK(orders_count >= )0,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0),
	rate_avg numeric(14, 5) NOT NULL DEFAULT 0 CHECK (rate_avg >= 0),
	order_processing_fee (14, 5) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0),
	courier_order_sum (14, 5) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0),
	courier_tips_sum (14, 5) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0),
	courier_reward_sum (14, 5) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0),
	UNIQUE(courier_id, settlement_year, settlement_month)
);