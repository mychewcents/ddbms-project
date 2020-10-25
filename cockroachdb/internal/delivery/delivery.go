package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"bufio"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type districtOrder struct {
	districtID, orderID int
}

// ProcessTransaction processes the Delivery transaction
func ProcessTransaction(db *sql.DB, scanner *bufio.Scanner, transactionArgs []string) {
	warehouseID, _ := strconv.Atoi(transactionArgs[0])
	carrierID, _ := strconv.Atoi(transactionArgs[1])
	execute(db, warehouseID, carrierID)
}

func execute(db *sql.DB, warehouseID int, carrierID int) {

	orderQuery := "SELECT O_ID FROM ORDERS_%d_%d WHERE O_CARRIER_ID=0 ORDER BY O_ID LIMIT 1"
	updateOrderQuery := "UPDATE ORDERS_%d_%d SET (O_CARRIER_ID, O_DELIVERY_D) = (%d, now()) WHERE O_W_ID=%d AND O_D_ID=%d AND O_ID=%d RETURNING O_C_ID, O_TOTAL_AMOUNT"
	updateCustomerQuery := "UPDATE CUSTOMER SET (C_BALANCE, C_DELIVERY_CNT) = (C_BALANCE + %f, C_DELIVERY_CNT + 1) WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d"
	
	var orders []districtOrder
	err := crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		for district := 1; district <= 10; district++ {
			var orderID sql.NullInt32
			if err := tx.QueryRow(fmt.Sprintf(orderQuery, warehouseID, district)).Scan(&orderID); err != nil && err != sql.ErrNoRows {
				return err
			}
			if orderID.Valid {
				orders = append(orders, districtOrder{district, int(orderID.Int32)})
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(orders) == 0 {
		return
	}
	err = crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		for _, order := range orders {
			districtID := order.districtID
			orderID := order.orderID
			var totalAmount float64
			var customerID int
			if err := tx.QueryRow(fmt.Sprintf(updateOrderQuery, warehouseID, districtID, carrierID, warehouseID, districtID, orderID)).Scan(&customerID, &totalAmount); err != nil {
				return err
			}
			if _, err := tx.Exec(fmt.Sprintf(updateCustomerQuery, totalAmount, warehouseID, districtID, customerID)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
