-- IMPORT INTO WAREHOUSE ( W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD ) 
-- CSV DATA ('nodelocal://1/project-files/data-files/warehouse.csv');

INSERT INTO WAREHOUSE SELECT * FROM WAREHOUSE_ORIG;

-- IMPORT INTO DISTRICT_ORIG ( D_W_ID, D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID ) 
-- CSV DATA ('nodelocal://1/project-files/data-files/district.csv');

INSERT INTO DISTRICT SELECT * FROM DISTRICT_ORIG;
ALTER TABLE DISTRICT ADD COLUMN D_W_TAX DECIMAL(4,4);
UPDATE DISTRICT SET D_W_TAX = W_TAX FROM WAREHOUSE WHERE DISTRICT.D_W_ID = WAREHOUSE.W_ID;

IMPORT INTO CUSTOMER ( C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA ) 
CSV DATA ('nodelocal://1/project-files/data-files/customer.csv');

-- INSERT INTO CUSTOMER SELECT * FROM CUSTOMER_ORIG;

IMPORT INTO ITEM ( I_ID, I_NAME, I_PRICE, I_IM_ID, I_DATA) CSV DATA ('nodelocal://1/project-files/data-files/item.csv');

-- INSERT INTO ITEM SELECT * FROM ITEM_ORIG;

IMPORT INTO STOCK (S_W_ID, S_I_ID, S_QUANTITY, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, S_DATA) 
CSV DATA ('nodelocal://1/project-files/data-files/stock.csv');

-- INSERT INTO STOCK SELECT * FROM STOCK_ORIG;

ALTER TABLE STOCK ADD COLUMN S_I_NAME STRING;
ALTER TABLE STOCK ADD COLUMN S_I_PRICE DECIMAL(5, 2);

UPDATE STOCK SET S_I_NAME = I_NAME, S_I_PRICE = I_PRICE FROM ITEM WHERE STOCK.S_I_ID = ITEM.I_ID;

IMPORT INTO ORDERS (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)
CSV DATA ('nodelocal://1/project-files/data-files/order.csv') WITH nullif='null'; 

-- INSERT INTO ORDERS SELECT * FROM ORDER_ORIG;
ALTER TABLE ORDERS ADD COLUMN O_TOTAL_AMOUNT DECIMAL(12, 2) DEFAULT 0.0;
ALTER TABLE ORDERS ADD COLUMN O_DELIVERY_D TIMESTAMP;

IMPORT INTO ORDER_LINE ( OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO) 
CSV DATA ('nodelocal://1/project-files/data-files/order-line.csv') WITH nullif='null';

-- INSERT INTO ORDER_LINE SELECT * FROM ORDER_LINE_ORIG;
