package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/mychewcents/ddbms-project/cockroachdb/internal/cdbconn"
)

var db *sql.DB

func init() {
	var err error
	db, err = cdbconn.CreateConnection("localhost", "26257", "defaultdb", "root")
	if err != nil {
		panic(err)
	}
}

func main() {
	// createOrdersTables(10, 10)
	createOrderLinesTables(10, 10)
}

func createOrdersTables(warehouses, districts int) {
	baseSQLStatement := `
		DROP TABLE IF EXISTS defaultdb.ORDERS_WID_DID;
		
		CREATE TABLE IF NOT EXISTS defaultdb.ORDERS_WID_DID (
			O_W_ID int,
			O_D_ID int,
			O_ID int,
			O_C_ID int NULL,
			O_CARRIER_ID int DEFAULT NULL,
			O_OL_CNT decimal(2,0),
			O_ALL_LOCAL DECIMAL(1,0),
			O_ENTRY_D timestamp DEFAULT CURRENT_TIMESTAMP,
			O_TOTAL_AMOUNT decimal(12,2),
			INDEX (O_C_ID),
			PRIMARY KEY (O_W_ID, O_D_ID, O_ID),
			CONSTRAINT FK_ORDERS FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES defaultdb.CUSTOMER (C_W_ID, C_D_ID, C_ID)
		);
		
		INSERT INTO defaultdb.ORDERS_WID_DID (O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D, O_TOTAL_AMOUNT) 
		SELECT O_W_ID, O_D_ID, O_ID, O_C_ID, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, O_ENTRY_D, 0.0 FROM defaultdb.ORDERS 
		WHERE O_W_ID = WID AND O_D_ID = DID;
	`

	for i := 1; i <= warehouses; i++ {
		for j := 1; j <= districts; j++ {
			finalSQLStatement := strings.ReplaceAll(baseSQLStatement, "WID", strconv.Itoa(i))
			finalSQLStatement = strings.ReplaceAll(finalSQLStatement, "DID", strconv.Itoa(j))

			// fmt.Println(finalSQLStatement)
			_, err := db.Exec(finalSQLStatement)
			if err != nil {
				fmt.Println(err)
			}

			fmt.Println("Compelete: ", i, j)
		}
	}
}

func createOrderLinesTables(warehouses, districts int) {
	baseSQLStatement := `
		DROP TABLE IF EXISTS defaultdb.ORDER_LINE_WID_DID;
		CREATE TABLE IF NOT EXISTS defaultdb.ORDER_LINE_WID_DID (
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
			INDEX (OL_I_ID),
			PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
		);
		
		INSERT INTO defaultdb.ORDER_LINE_WID_DID 
		SELECT * FROM defaultdb.ORDER_LINE
		WHERE OL_W_ID = WID AND OL_D_ID = DID;
	`

	for i := 1; i <= warehouses; i++ {
		for j := 1; j <= districts; j++ {
			finalSQLStatement := strings.ReplaceAll(baseSQLStatement, "WID", strconv.Itoa(i))
			finalSQLStatement = strings.ReplaceAll(finalSQLStatement, "DID", strconv.Itoa(j))

			// fmt.Println(finalSQLStatement)
			_, err := db.Exec(finalSQLStatement)
			if err != nil {
				fmt.Println(err)
			}

			fmt.Println("Compelete: ", i, j)
		}
	}
}
