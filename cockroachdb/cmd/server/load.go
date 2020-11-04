package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/mychewcents/ddbms-project/cockroachdb/internal/connection/cdbconn"
	"github.com/mychewcents/ddbms-project/cockroachdb/internal/connection/config"
	"github.com/mychewcents/ddbms-project/cockroachdb/internal/init/tables"
)

func load(c config.Configuration) {

	db, err := cdbconn.CreateConnection(c.HostNode)
	if err != nil {
		panic("load function couldn't create a connection to the server")
	}

	fmt.Printf("Executing the SQL: scripts/sql/drop-partitions.sql")
	if err := tables.ExecuteSQLForPartitions(db, 10, 10, "scripts/sql/drop-partitions.sql"); err != nil {
		fmt.Println(err)
		return
	}

	sqlScripts := []string{
		"scripts/sql/drop-raw.sql",
		"scripts/sql/create-raw.sql",
		"scripts/sql/load-raw.sql",
		"scripts/sql/update-raw.sql",
	}

	for _, value := range sqlScripts {
		fmt.Printf("\nExecuting the SQL: %s", value)
		if err := tables.ExecuteSQL(db, value); err != nil {
			fmt.Println(err)
			return
		}
	}

	sqlScripts = []string{
		"scripts/sql/create-partitions.sql",
		"scripts/sql/load-partitions.sql",
		"scripts/sql/update-partitions.sql",
	}

	for _, value := range sqlScripts {
		fmt.Printf("\nExecuting the SQL: %s", value)
		if err := tables.ExecuteSQLForPartitions(db, 10, 10, value); err != nil {
			fmt.Println(err)
			return
		}
	}

	if err := loadOrderItemsCustomerPair(db, 10); err != nil {
		log.Fatalf("error in loadOrderItemsCustomerPair. Err: %v", err)
	}
	log.Println("Initialization Complete!")
	fmt.Println("\nInitialization Complete!")
}

func loadOrderItemsCustomerPair(db *sql.DB, warehouses int) error {
	log.Println("Executing the load of Items Customer Pair")
	for w := 1; w <= warehouses; w++ {
		for d := 1; d <= 10; d++ {
			orderItemCustomerPairTable := fmt.Sprintf("ORDER_ITEMS_CUSTOMERS_%d_%d", w, d)
			orderLineTable := fmt.Sprintf("ORDER_LINE_%d_%d", w, d)
			orderTable := fmt.Sprintf("ORDERS_%d_%d", w, d)

			for c := 1; c <= 3000; c++ {
				var orderID, orderLineItemsCount int

				sqlStatement := fmt.Sprintf("SELECT O_ID, O_OL_CNT FROM %s WHERE O_C_ID = %d", orderTable, c)

				row := db.QueryRow(sqlStatement)
				if err := row.Scan(&orderID, &orderLineItemsCount); err != nil {
					log.Fatalf("error in getting the order id for w = %d d = %d c = %d", w, d, c)
				}

				sqlStatement = fmt.Sprintf("SELECT OL_I_ID FROM %s WHERE OL_O_ID = %d", orderLineTable, orderID)
				rows, err := db.Query(sqlStatement)
				if err != nil {
					log.Fatalf("error in getting the order line items for w = %d d = %d orderid = %d", w, d, orderID)
				}

				var orderLineItem, totalUniqueItems int
				orderLineItems := make([]int, orderLineItemsCount)
				orderLineItemMap := make(map[int]bool)
				for rows.Next() {
					err := rows.Scan(&orderLineItem)
					if err != nil {
						log.Fatalf("error reading the output of the order line number for w = %d d = %d orderid = %d", w, d, orderID)
					}

					if _, ok := orderLineItemMap[orderLineItem]; !ok {
						orderLineItemMap[orderLineItem] = true
						orderLineItems[totalUniqueItems] = orderLineItem
						totalUniqueItems++
					}
				}
				orderLineItems = orderLineItems[:totalUniqueItems]
				sort.Ints(orderLineItems)

				var orderItemCustomerPair strings.Builder

				for i := 0; i < len(orderLineItems)-1; i++ {
					for j := i + 1; j < len(orderLineItems); j++ {
						orderItemCustomerPair.WriteString(fmt.Sprintf("(%d, %d, %d, %d, %d),", w, d, c, orderLineItems[i], orderLineItems[j]))
					}
				}

				sqlStatement = fmt.Sprintf("UPSERT INTO %s (IC_W_ID, IC_D_ID, IC_C_ID, IC_I_1_ID, IC_I_2_ID) VALUES %s", orderItemCustomerPairTable, orderItemCustomerPair.String())
				sqlStatement = sqlStatement[0 : len(sqlStatement)-1]

				if _, err := db.Exec(sqlStatement); err != nil {
					return err
				}
				log.Printf("Executed partition: %d %d %d", w, d, c)
			}

		}
	}

	log.Println("Completed the load of Items Customer Pair")

	return nil
}

func loadCSV(c config.Configuration) {
	sqlScript := "scripts/sql/load-partitions-csv.sql"

	sqlFile, err := os.Open(sqlScript)
	if err != nil {
		log.Fatalf("Err: %v", err)
		return
	}
	defer sqlFile.Close()

	byteValue, _ := ioutil.ReadAll(sqlFile)
	baseSQLStatement := string(byteValue)

	db, err := cdbconn.CreateConnection(c.HostNode)
	if err != nil {
		panic("load function couldn't create a connection to the server")
	}

	for w := 1; w <= 10; w++ {
		for d := 1; d <= 10; d++ {
			finalSQLStatement := strings.ReplaceAll(baseSQLStatement, "ORDERS_FILE_PATH", fmt.Sprintf("order/%d_%d", w, d))
			finalSQLStatement = strings.ReplaceAll(finalSQLStatement, "ORDER_LINE_FILE_PATH", fmt.Sprintf("orderline/%d_%d", w, d))
			finalSQLStatement = strings.ReplaceAll(finalSQLStatement, "ORDER_ITEMS_CUSTOMERS_FILE_PATH", fmt.Sprintf("itempairs/%d_%d", w, d))

			_, err := db.Exec(finalSQLStatement)
			if err != nil {
				log.Fatalf("couldn't load the table: %d %d. Err: %v", w, d, err)
			}
			log.Printf("Completed Partition: %d %d", w, d)
		}
	}

}
