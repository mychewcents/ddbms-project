package delivery

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"github.com/cockroachdb/cockroach-go/crdb"
)

type districtOrder struct {
	district, orderId int
}

func ProcessTransaction(db *sql.DB, warehouseId int, carrierId int) {
	orderQuery := "SELECT O_ID FROM ORDERS_%d_%d WHERE O_CARRIER_ID=0 ORDER BY O_ID LIMIT 1"
	updateOrderQuery := "UPDATE ORDERS_%d_%d SET (O_CARRIER_ID, O_DELIVERY_D) = (%d, now()) WHERE O_W_ID=%d AND O_D_ID=%d AND O_ID=%d RETURNING O_C_ID, O_TOTAL_AMOUNT"
	updateCustomerQuery := "UPDATE CUSTOMER SET (C_BALANCE, C_DELIVERY_CNT) = (C_BALANCE + %f, C_DELIVERY_CNT + 1) WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d"
	var orders []districtOrder
	err := crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		for district := 1; district <= 10; district++ {
			var orderId sql.NullInt32
			if err := tx.QueryRow(fmt.Sprintf(orderQuery, warehouseId, district)).Scan(&orderId); err != sql.ErrNoRows {
				return err
			}
			if orderId.Valid {
				orders = append(orders, districtOrder{district, int(orderId.Int32)})
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
			district := order.district
			orderId := order.orderId
			var totalAmount float64
			var customerId int
			if err := tx.QueryRow(fmt.Sprintf(updateOrderQuery, warehouseId, district, carrierId, warehouseId, district, orderId)).Scan(&customerId, &totalAmount); err != nil {
				return err
			}
			if _, err := tx.Exec(fmt.Sprintf(updateCustomerQuery, totalAmount, warehouseId, district, customerId)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
