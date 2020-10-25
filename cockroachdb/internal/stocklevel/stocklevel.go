package stocklevel

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"strconv"
)

// ProcessTransaction processes the Stock Level Transaction
func ProcessTransaction(db *sql.DB, scanner *bufio.Scanner, transactionArgs []string) {
	warehouseID, _ := strconv.Atoi(transactionArgs[1])
	districtID, _ := strconv.Atoi(transactionArgs[2])
	threshold, _ := strconv.Atoi(transactionArgs[3])
	lastNOrders, _ := strconv.Atoi(transactionArgs[4])

	execute(db, warehouseID, districtID, threshold, lastNOrders)
}

func execute(db *sql.DB, warehouseID, districtID, threshold, lastNOrders int) {
	var totalItems, lastOrderID int

	row := db.QueryRow("SELECT d_next_o_id FROM district WHERE d_w_id=$1 AND d_id=$2", warehouseID, districtID)

	if err := row.Scan(&lastOrderID); err != nil {
		log.Fatalf("%v", err)
		return
	}

	startOrderID := lastOrderID - lastNOrders

	sqlStatement := fmt.Sprintf(`
		SELECT COUNT(*) FROM Stock 
		WHERE S_W_ID=%d 
		AND S_QUANTITY < %d 
		AND S_I_ID IN (
			SELECT OL_I_ID FROM ORDER_LINE_%d_%d 
			WHERE OL_O_ID < %d AND OL_O_ID >= %d
		)`,
		warehouseID, threshold, warehouseID, districtID, lastOrderID, startOrderID,
	)

	row = db.QueryRow(sqlStatement)

	if err := row.Scan(&totalItems); err != nil {
		log.Fatalf("%v", err)
		return
	}

	printOutputState(totalItems, lastOrderID-lastNOrders, lastOrderID)

}

func printOutputState(totalItems, startOrderID, endOrderID int) {
	fmt.Println(fmt.Sprintf("Total Number of Items below threshold: %d , for Order IDs between %d - %d", totalItems, startOrderID, endOrderID))
}
