package caller

import (
	"bufio"
	"database/sql"

	"github.com/mychewcents/ddbms-project/cockroachdb/internal/orderstatus"
	"github.com/mychewcents/ddbms-project/cockroachdb/internal/payment"

	"github.com/mychewcents/ddbms-project/cockroachdb/internal/neworder"
	"github.com/mychewcents/ddbms-project/cockroachdb/internal/popularitem"
	"github.com/mychewcents/ddbms-project/cockroachdb/internal/stocklevel"
	"github.com/mychewcents/ddbms-project/cockroachdb/internal/topbalance"
)

// ProcessRequest Calls the required DB function
func ProcessRequest(db *sql.DB, scanner *bufio.Scanner, transactionArgs []string) {

	switch transactionArgs[0] {
	case "N":
		neworder.ProcessTransaction(db, scanner, transactionArgs[1:])
	case "P":
		payment.ProcessTransaction(db, nil, transactionArgs[1:])
	case "D":

	case "O":
		orderstatus.ProcessTransaction(db, nil, transactionArgs[1:])
	case "S":
		stocklevel.ProcessTransaction(db, nil, transactionArgs[1:])
	case "I":
		popularitem.ProcessTransaction(db, nil, transactionArgs[1:])
	case "T":
		topbalance.ProcessTransaction(db, nil)
	case "R":

	}

}
