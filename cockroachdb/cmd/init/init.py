import time
import random
import psycopg2
from psycopg2.errors import SerializationFailure
import logging
import math
import pandas as pd

CREATE_ORDERS_ORIG = """CREATE TABLE IF NOT EXISTS ORDERS_ORIG_{}_{} (
O_W_ID int,
O_D_ID int,
O_ID int,
O_C_ID int NULL,
O_CARRIER_ID int,
O_OL_CNT decimal(2,0),
O_ALL_LOCAL DECIMAL(1,0),
O_ENTRY_D timestamp,
PRIMARY KEY (O_W_ID, O_D_ID, O_ID),
CONSTRAINT FK_ORDERS FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES CUSTOMER_ORIG (C_W_ID, C_D_ID, C_ID)
);
"""

INSERT_ORDERS_ORIG = """UPSERT INTO ORDERS_ORIG_{}_{} (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D)
VALUES({}, {}, {}, {}, {}, {}, {}, '{}')
"""

CREATE_ORDER_LINES_ORIG = """CREATE TABLE IF NOT EXISTS ORDER_LINES_ORIG_{}_{} (
  OL_W_ID int,
  OL_D_ID int,
  OL_O_ID int,
  OL_NUMBER int,
  OL_I_ID int,
  OL_DELIVERY_D timestamp,
  OL_AMOUNT decimal(6,2),
  OL_SUPPLY_W_ID int,
  OL_QUANTITY decimal(2,0),
  OL_DIST_INFO char(24),
  INDEX (OL_O_ID),
  PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
  CONSTRAINT FK_ORDER_LINES FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES ORDER_ORIG (O_W_ID, O_D_ID, O_ID)
);
"""

INSERT_ORDER_LINES_ORIG = """UPSERT INTO ORDER_LINES_ORIG_{}_{} (
  OL_W_ID,
  OL_D_ID,
  OL_O_ID,
  OL_NUMBER,
  OL_I_ID,
  OL_DELIVERY_D,
  OL_AMOUNT,
  OL_SUPPLY_W_ID,
  OL_QUANTITY,
  OL_DIST_INFO
)
VALUES({},{},{},{},{},'{}',{},{},{},'{}');
"""

CREATE_ORDER_LINES = """CREATE TABLE IF NOT EXISTS ORDER_LINES_{}_{} (
  OL_W_ID int,
  OL_D_ID int,
  OL_O_ID int,
  OL_NUMBER int,
  OL_I_ID int,
  OL_DELIVERY_D timestamp,
  OL_AMOUNT decimal(6,2),
  OL_SUPPLY_W_ID int,
  OL_QUANTITY decimal(2,0),
  OL_DIST_INFO char(24),
  INDEX (OL_O_ID),
  PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
  CONSTRAINT FK_ORDER_LINES FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES ORDER_ORIG (O_W_ID, O_D_ID, O_ID)
);
"""

INSERT_ORDER_LINES = """UPSERT INTO ORDER_LINES_{}_{} (
  OL_W_ID,
  OL_D_ID,
  OL_O_ID,
  OL_NUMBER,
  OL_I_ID,
  OL_DELIVERY_D,
  OL_AMOUNT,
  OL_SUPPLY_W_ID,
  OL_QUANTITY,
  OL_DIST_INFO
)
VALUES({},{},{},{},{},'{}',{},{},{},'{}');
"""


def createOrderLines(conn):
	df = pd.read_csv("/home/stuproj/cs4224m/downloads/project-files/data-files/district.csv")
	with conn.cursor() as cur:
		for ind, row in df.iterrows():
			cur.execute(CREATE_ORDER_LINES.format(row[0], row[1]))
	conn.commit()

def createOrders(conn):
	df = pd.read_csv("/home/stuproj/cs4224m/downloads/project-files/data-files/district.csv")
	with conn.cursor() as cur:
		for ind, row in df.iterrows():
			cur.execute(CREATE_ORDERS.format(row[0], row[1]))
	conn.commit()

def insertOrders(conn):
	df = pd.read_csv("/home/stuproj/cs4224m/downloads/project-files/data-files/order.csv")
	cnt = 0
	with conn.cursor() as cur:
		for ind, row in df.iterrows():
			cur.execute(INSERT_ORDERS.format(row[0], row[1], row[0], row[1], row[2], row[3], 0 if math.isnan(row[4]) else row[4], row[5], row[6], row[7]))
			cnt += 1
			if cnt % 1000 == 0:
				logging.debug("inserted %d rows", cnt)
	conn.commit()

def insertOrderLines(conn):
	df = pd.read_csv("/home/stuproj/cs4224m/downloads/project-files/data-files/order-line.csv")
	with conn.cursor() as cur:
		cnt = 0
		for ind, row in df.iterrows():
			cur.execute(INSERT_ORDER_LINES.format(row[0], row[1], row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]))
			cnt += 1
			if cnt % 1000 == 0:
				logging.debug("inserted %d rows", cnt)
	conn.commit()

def run_transaction(conn, op, max_retries=3):
    """
    Execute the operation *op(conn)* retrying serialization failure.

    If the database returns an error asking to retry the transaction, retry it
    *max_retries* times before giving up (and propagate it).
    """
    # leaving this block the transaction will commit or rollback
    # (if leaving with an exception)
    with conn:
        for retry in range(1, max_retries + 1):
            try:
                op(conn)

                # If we reach this point, we were able to commit, so we break
                # from the retry loop.
                return

            except SerializationFailure as e:
                # This is a retry error, so we roll back the current
                # transaction and sleep for a bit before retrying. The
                # sleep time increases for each failed transaction.
                logging.debug("got error: %s", e)
                conn.rollback()
                logging.debug("EXECUTE SERIALIZATION_FAILURE BRANCH")
                sleep_ms = (2 ** retry) * 0.1 * (random.random() + 0.5)
                logging.debug("Sleeping %s seconds", sleep_ms)
                time.sleep(sleep_ms)

            except psycopg2.Error as e:
                logging.debug("got error: %s", e)
                logging.debug("EXECUTE NON-SERIALIZATION_FAILURE BRANCH")
                raise e
        raise ValueError(f"Transaction did not succeed after {max_retries} retries")

if __name__ == "__main__":
	conn = psycopg2.connect("postgresql://root@192.168.48.179:27000/defaultdb?sslmode=disable")
	logging.basicConfig(filename="init.log", level=logging.DEBUG)
	createOrderLines(conn)

	try:
		run_transaction(conn, lambda conn: insertOrderLines(conn))

	except ValueError as ve:
		# Below, we print the error and continue on so this example is easy to
		# run (and run, and run...).  In real code you should handle this error
		# and any others thrown by the database interaction.
		logging.debug("run_transaction(conn, op) failed: %s", ve)
		pass

	conn.close()
