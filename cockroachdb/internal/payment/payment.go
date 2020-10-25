package payment

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"

	"github.com/cockroachdb/cockroach-go/crdb"
)

// ProcessTransaction processes the Payment transaction
func ProcessTransaction(db *sql.DB, scanner *bufio.Scanner, transactionArgs []string) {
	warehouseID, _ := strconv.Atoi(transactionArgs[0])
	districtID, _ := strconv.Atoi(transactionArgs[1])
	customerID, _ := strconv.Atoi(transactionArgs[2])
	paymentAmt, _ := strconv.ParseFloat(transactionArgs[3], 64)
	execute(db, warehouseID, districtID, customerID, paymentAmt)
}

func execute(db *sql.DB, customerWHID int, customerDistrictID int, customerID int, payment float64) {
	// QUERIES
	updateDistrict := fmt.Sprintf(`UPDATE DISTRICT SET D_YTD = D_YTD + %f WHERE D_W_ID = %d AND D_ID = %d RETURNING D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP`, 
	payment, customerWHID, customerDistrictID)
	
	updateCustomer := fmt.Sprintf(`UPDATE CUSTOMER SET (C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT) = (C_BALANCE - %f, C_YTD_PAYMENT + %f, C_PAYMENT_CNT + 1)
	WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d RETURNING C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
	C_PHONE, C_SINCE, C_CREDIT,C_CREDIT_LIM, C_DISCOUNT, C_BALANCE`, payment, payment, customerWHID, customerDistrictID, customerID);
	
	readWarehouse := fmt.Sprintf("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = %d", customerWHID)

	var dStreet1, dStreet2, dCity, dState, dZip, firstName, middleName, lastName, cStreet1, cStreet2, cCity, cState, cZip,
		cPhone, cSince, cCredit, cCreditLimit, cDiscount, cBalance, wStreet1, wStreet2, wCity, wState, wZip string

	err := crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		if err := tx.QueryRow(updateDistrict).Scan(&dStreet1, &dStreet2, &dCity, &dState, &dZip); err != nil {
			return err
		}
		if err := tx.QueryRow(updateCustomer).Scan(&firstName, &middleName, &lastName, &cStreet1, &cStreet2, &cCity, &cState, &cZip,
			&cPhone, &cSince, &cCredit, &cCreditLimit, &cDiscount, &cBalance); err != nil {
			return err
		}
		if err := tx.QueryRow(readWarehouse).Scan(&wStreet1, &wStreet2, &wCity, &wState, &wZip); err != nil {
			return err
		}
		return nil
	})

	if err == sql.ErrNoRows {
		fmt.Println("No records found!")
		return
	}
	if err != nil {
		log.Fatalf("%v", err)
	}

	outputStr := "Customer identifier: (%d, %d, %d)\nName: (%s, %s, %s)\nAddress: (%s, %s, %s, %s, %s)\nPhone: %s\nMember Since:%s\nCredit and Limit: (%s, %s)\nDiscount: %s\nBalance: %s"
	outputStr += "\nWarehouse address: (%s, %s, %s, %s, %s)\nDistrict address: (%s, %s, %s, %s, %s)\nPayment: %f"
	output := fmt.Sprintf(outputStr,
		customerWHID, customerDistrictID, customerID,
		firstName, middleName, lastName,
		cStreet1, cStreet2, cCity, cState, cZip,
		cPhone,
		cSince,
		cCredit, cCreditLimit,
		cDiscount,
		cBalance,
		wStreet1, wStreet2, wCity, wState, wZip,
		dStreet1, dStreet2, dCity, dState, dZip,
		payment
	)

	fmt.Println(output)
}
