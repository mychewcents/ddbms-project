package topbalance

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/cockroachdb/cockroach-go/crdb"
)

type txnOutput struct {
	first, middle, last, warehouseName, districtName string
	warehouseId, districtId                          int
	balance                                          float64
}

func ProcessTransaction(db *sql.DB, scanner *bufio.Scanner) {
	execute(db)
}

func execute(db *sql.DB) {
	// Index on C_BALANCE
	customerQuery := "SELECT C_FIRST, C_MIDDLE, C_LAST, C_W_ID, C_D_ID, C_BALANCE FROM CUSTOMER ORDER BY C_BALANCE DESC LIMIT 10";
	districtQuery := "SELECT D_NAME FROM DISTRICT WHERE D_W_ID=%d AND D_ID=%d"
	warehouseQuery := "SELECT W_NAME FROM WAREHOUSE WHERE W_ID=%d"

	var customers []txnOutput
	err := crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		rows, err := tx.Query(customerQuery)
		if err != nil {
			return err
		}
		for rows.Next() {
			var customer txnOutput
			if err := rows.Scan(&customer.first, &customer.middle, &customer.last, &customer.warehouseId, &customer.districtId, &customer.balance); err != nil {
				return err
			}
			customers = append(customers, customer)
		}
		for i, _ := range customers {
			if err := db.QueryRow(fmt.Sprintf(districtQuery, customers[i].warehouseId, customers[i].districtId)).Scan(&customers[i].districtName); err != nil {
				return err
			}
			if err := db.QueryRow(fmt.Sprintf(warehouseQuery, customers[i].warehouseId)).Scan(&customers[i].warehouseName); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	for _, customer := range customers {
		fmt.Println(fmt.Sprintf("Customer: %s %s %s, Balance: %f, Warehouse: %s, District: %s",
			customer.first, customer.middle, customer.last, customer.balance, customer.warehouseName, customer.districtName))
	}
}
