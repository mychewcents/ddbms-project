package main

import (
	"database/sql"
	"fmt"
	"github.com/mychewcents/ddbms-project/cockroachdb/internal/cdbconn"
	"github.com/mychewcents/ddbms-project/cockroachdb/internal/payment"
)

var db *sql.DB

func init() {
	var err error
	db, err = cdbconn.CreateConnection()
	if err != nil {
		panic(err)
	}
}

func main() {

	for true {

	var transaction_type byte
	_, err := fmt.Scanf("%c,", &transaction_type)

	if err != nil {
		if err.Error() == "EOF" {
			fmt.Println("Read EOF")
		} else {
			fmt.Println(err)
		}
		break
	}

	switch (transaction_type) {
		case 'N':
		case 'P':
			var warehouseId, districtId, customerId int
				var amount float64
			fmt.Scanf("%d,%d,%d,%f", &warehouseId, &districtId, &customerId, &amount)
			payment.ProcessTransaction(db, warehouseId, districtId, customerId, amount)
			break
		case 'D':
		case 'O':
		case 'S':
		case 'I':
		case 'T':
		case 'R':

		}
	}
}
