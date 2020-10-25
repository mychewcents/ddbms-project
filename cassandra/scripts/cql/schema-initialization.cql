-- Create keyspace

DROP KEYSPACE IF EXISTS cassandra;
CREATE KEYSPACE IF NOT EXISTS cassandra WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};

USE cassandra;

-- creating the userdefined types tables

CREATE TYPE address (
	STREET_1 text,
	STREET_2 text,
	CITY text,
	STATE text,
	ZIP text
);

CREATE TYPE name (
    FIRST_NAME text,
	MIDDLE_NAME text,
	LAST_NAME text
);

-- Create the base tables

CREATE TABLE warehouse_tab (
	W_ID int,
	W_NAME text,
	W_ADDRESS address,
	W_TAX float,
	W_YTD double,
	PRIMARY KEY (W_ID)
);

CREATE TABLE district_tab (
	D_W_ID int,
	D_ID int,
    D_NAME text, 
	D_ADDRESS address,
	D_TAX float,
	D_YTD double,
	PRIMARY KEY ((D_W_ID, D_ID))
);

CREATE TABLE customer_tab (
	C_W_ID int,
	C_D_ID int, 
	C_ID int,
	C_W_NAME text,
	C_W_TAX float,
	C_D_NAME text,
	C_D_TAX float,
    C_NAME name,
	C_ADDRESS address,
	C_PHONE text,
	C_SINCE timestamp,
	C_CREDIT text,
	C_CREDIT_LIM double,
	C_DISCOUNT float,
	C_BALANCE double,
	C_YTD_PAYMENT double,
	C_PAYMENT_CNT int,
	C_DELIVERY_CNT int,
	C_DATA text,
	PRIMARY KEY (C_W_ID, C_D_ID, C_ID)
);

-- Create the materialized view(s) for top balance

CREATE MATERIALIZED VIEW customer_by_balance AS
SELECT C_W_ID, C_BALANCE, C_D_ID, C_ID, C_W_NAME, C_D_NAME, C_NAME FROM customer_tab
WHERE c_w_id IS NOT NULL 
AND c_balance IS NOT NULL 
AND c_d_id IS NOT NULL 
AND c_id IS NOT NULL 
AND c_w_name IS NOT NULL 
AND c_d_name IS NOT NULL 
AND c_name IS NOT NULL 
PRIMARY KEY ((C_W_ID), C_BALANCE, C_D_ID, C_ID)
WITH CLUSTERING ORDER BY (C_BALANCE DESC);

-- Create the base table for order

CREATE TABLE order_tab (
	O_W_ID int,
	O_D_ID int, 
	O_ID timeuuid,
	O_C_ID int,
	O_C_NAME text,
	O_CARRIER_ID int,
	OL_DELIVERY_D timestamp,
	O_OL_COUNT int,
	O_OL_TOTAL_AMOUNT double,
	O_ALL_LOCAL boolean,
	O_ENTRY_D timestamp,
	PRIMARY KEY ((O_W_ID, O_D_ID), O_ID),
) WITH CLUSTERING ORDER BY (O_ID DESC);


-- Create the materialized view(s) for carrier_view and customer_view

CREATE MATERIALIZED VIEW order_by_carrier_view AS
SELECT O_W_ID, O_D_ID, O_CARRIER_ID, O_ID, O_C_ID, O_OL_TOTAL_AMOUNT FROM order_tab
WHERE O_W_ID IS NOT NULL
AND O_D_ID IS NOT NULL
AND O_CARRIER_ID IS NOT NULL
AND O_ID IS NOT NULL
AND O_C_ID IS NOT NULL
AND O_OL_TOTAL_AMOUNT IS NOT NULL
PRIMARY KEY ((O_W_ID, O_D_ID), O_CARRIER_ID, O_ID)
WITH CLUSTERING ORDER BY (O_ID ASC);

	
CREATE MATERIALIZED VIEW order_by_customer_view AS
SELECT O_W_ID, O_D_ID, O_C_ID, O_ID, O_ENTRY_D, O_CARRIER_ID, OL_DELIVERY_D FROM order_tab
WHERE o_w_id IS NOT NULL 
AND o_d_id IS NOT NULL 
AND o_c_id IS NOT NULL 
AND o_id IS NOT NULL 
AND o_entry_d IS NOT NULL 
AND o_carrier_id IS NOT NULL 
AND ol_delivery_d IS NOT NULL 
PRIMARY KEY ((O_W_ID, O_D_ID), O_C_ID, O_ID)
WITH CLUSTERING ORDER BY (O_ID DESC);

-- Create the base table for order_line and stock

CREATE TABLE order_line_tab (
	OL_W_ID int,
	OL_D_ID int,
	OL_O_ID timeuuid,
	OL_QUANTITY int,
	OL_NUMBER int,
	OL_I_ID int,
	OL_I_NAME text,
	OL_AMOUNT float,
	OL_SUPPLY_W_ID int,
	OL_DIST_INFO text,
	PRIMARY KEY ((OL_W_ID, OL_D_ID), OL_O_ID, OL_QUANTITY, OL_NUMBER)
) WITH CLUSTERING ORDER BY (OL_O_ID ASC, OL_QUANTITY DESC);

CREATE TABLE stock_tab (
	S_W_ID int,
	S_I_ID int,
	S_QUANTITY int,
	S_I_NAME text,
	S_I_PRICE float,
	S_I_IM_ID int,
	S_I_DATA text,
	S_YTD bigint,
	S_ORDER_CNT int,
	S_REMOTE_CNT int,
	S_DIST_01 text,
	S_DIST_02 text,
	S_DIST_03 text,
	S_DIST_04 text,
	S_DIST_05 text,
	S_DIST_06 text,
	S_DIST_07 text,
	S_DIST_08 text,
	S_DIST_09 text,
	S_DIST_10 text,
	S_DATA text,
	PRIMARY KEY (S_W_ID, S_I_ID, S_QUANTITY)
);

CREATE MATERIALIZED VIEW stock_tab_by_quantity_view AS
SELECT S_W_ID, S_I_ID, S_QUANTITY FROM stock_tab
WHERE S_W_ID IS NOT NULL
  AND S_I_ID IS NOT NULL
  AND S_QUANTITY IS NOT NULL
PRIMARY KEY (S_W_ID, S_I_ID, S_QUANTITY)
WITH CLUSTERING ORDER BY (S_QUANTITY DESC);