IMPORT INTO ORDERS_WID_DID CSV DATA ('nodelocal://1/assets/processed/ORDERS_FILE_PATH.csv') WITH skip = '1';
IMPORT INTO ORDER_LINE_WID_DID CSV DATA ('nodelocal://1/assets/processed/ORDER_LINE_FILE_PATH.csv') WITH skip = '1';
IMPORT INTO ORDER_ITEMS_CUSTOMERS_WID_DID CSV DATA ('nodelocal://1/assets/processed/ORDER_ITEMS_CUSTOMERS_FILE_PATH.csv') WITH skip = '1';