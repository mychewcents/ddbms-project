ALTER TABLE DISTRICT ADD COLUMN D_W_TAX DECIMAL(4,4);
UPDATE DISTRICT SET D_W_TAX = W_TAX FROM WAREHOUSE WHERE DISTRICT.D_W_ID = WAREHOUSE.W_ID;

ALTER TABLE ORDERS ADD COLUMN O_TOTAL_AMOUNT DECIMAL(12, 2) DEFAULT 0.0;
ALTER TABLE ORDERS ADD COLUMN O_DELIVERY_D TIMESTAMP;
ALTER TABLE ORDERS ADD COLUMN O_OL_ITEM_IDS STRING;

ALTER TABLE STOCK ADD COLUMN S_I_NAME STRING;
ALTER TABLE STOCK ADD COLUMN S_I_PRICE DECIMAL(5, 2);
UPDATE STOCK SET S_I_NAME = I_NAME, S_I_PRICE = I_PRICE FROM ITEM WHERE STOCK.S_I_ID = ITEM.I_ID;