package payment
import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"github.com/cockroachdb/cockroach-go/crdb"
)

func ProcessTransaction(db *sql.DB, customerWHId int, customerDistrictId int, customerId int, payment float64) {
	// QUERIES
	updateDistrict := fmt.Sprintf(`UPDATE DISTRICT_ORIG SET D_YTD = D_YTD + %f WHERE D_W_ID = %d AND D_ID = %d RETURNING D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP`, 
	payment, customerWHId, customerDistrictId)
	
	updateCustomer := fmt.Sprintf(`UPDATE CUSTOMER_ORIG SET (C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT) = (C_BALANCE + %f, C_YTD_PAYMENT + %f, C_PAYMENT_CNT + 1)
	WHERE C_W_ID = %d AND C_D_ID = %d AND C_ID = %d RETURNING C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
	C_PHONE, C_SINCE, C_CREDIT,C_CREDIT_LIM, C_DISCOUNT, C_BALANCE`, payment, payment, customerWHId, customerDistrictId, customerId);
	
	readWarehouse := fmt.Sprintf("SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE_ORIG WHERE W_ID = %d", customerWHId)

	var dStreet1, dStreet2, dCity, dState, dZip, firstName, middleName, lastName, cStreet1, cStreet2, cCity, cState, cZip,
		cPhone, cSince, cCredit, cCreditLimit, cDiscount, cBalance, wStreet1, wStreet2, wCity, wState, wZip string
	
	// Execute atomically
	err := crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		fmt.Println(updateDistrict)
		if err := tx.QueryRow(updateDistrict).Scan(&dStreet1, &dStreet2, &dCity, &dState, &dZip); err != nil {
			return err
		}
		fmt.Println(updateCustomer)
		if err := tx.QueryRow(updateCustomer).Scan(&firstName, &middleName, &lastName, &cStreet1, &cStreet2, &cCity, &cState, &cZip,
		&cPhone, &cSince, &cCredit, &cCreditLimit, &cDiscount, &cBalance); err != nil {
			return err
		}
		fmt.Println(readWarehouse)
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

	output := fmt.Sprintf("Customer identifier: (%d, %d, %d)\nWarehouse address: (%s, %s, %s, %s, %s)\nDistrict address: (%s, %s, %s, %s, %s)\nPayment: %f", 
	customerWHId, customerDistrictId, customerId,
	wStreet1, wStreet2, wCity, wState, wZip,
	dStreet1, dStreet2, dCity, dState, dZip,
	payment)

	fmt.Println(output)
}
