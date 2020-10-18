package payment
import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/cockroach-go/crdb"
)

func ProcessTransaction(db *sql.DB, customerWHId int, customerDistrictId int, customerId int, payment float64) {
	// QUERIES
	updateDistrict := fmt.Sprintf("UPDATE DISTRICT SET D_YTD = D_YTD + %f WHERE W_ID = %d AND D_ID = %d RETURNING DSTREET1, DSTREET2, DCITY, DSTATE, DZIP", 
	payment, customerWHId, customerDistrictId)
	
	updateCustomer := fmt.Sprintf("UPDATE CUSTOMER SET (C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT) = (C_BALANCE + %f, C_YTD_PAYMENT + %f, C_PAYMENT_CNT + 1) 
	WHERE C_W_ID = %d, C_D_ID = %d, C_ID = %d RETURNING C_FIRST, C_MIDDLE, C_LAST, CSTREET1, CSTREET2, CCITY, CSTATE, CZIP, 
	CPHONE, CSINCE, CCREDIT,CCREDITLIM, CDISCOUNT, CBALANCE", payment, payment, customerWHId, customerDistrictId, customerId);
	
	readWarehouse := fmt.Sprintf("SELECT WSTREET1, WSTREET2, WCITY, WSTATE, WZIP WHERE W_ID = %d", customerWHId)

	var dStreet1, dStreet2, dCity, dState, dZip, firstName, middleName, lastName, cStreet1, cStreet2, cCity, cState, cZip
		cPhone, cSince, cCredit, cCreditLimit, cDiscount, cBalance, wStreet1, wStreet2, sCity, wState, wZip string
	
	// Execute atomically
	err = crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
        if err := tx.QueryRow(updateDistrict).Scan(&dStreet1, &dStreet2, &dCity, &dState, &dZip); err != nil {
        	return err
        }
        if err := tx.QueryRow(updateCustomer).Scan(&firstName, &middleName, &lastName, &cStreet1, &cStreet2, &cCity, &cState, &cZip
		&cPhone, &cSince, &cCredit, &cCreditLimit, &cDiscount, &cBalance); err != nil {
        	return err
		}

		if err := tx.QueryRow(readWarehouse).Scan(&wStreet1, &wStreet2, &sCity, &wState, &wZip); err != nil {
			return err
		}
    })
    
    if err != nil {
    	log.Fatalf("%v", err)
    }

    output := fmt.SPrintf("Customer identifier: (%s, %s, %s)\n 
    	Warehouse address: (%s, %s, %s, %s, %s)\n 
    	District address: (%s, %s, %s, %s, %s)\n
    	Payment: %f", 
    	customerWHId, customerDistrictId, customerId,
    	wStreet1, wStreet2, wCity, wState, wZip,
    	dStreet1, dStreet2, dCity, dState, dZip,
    	payment)

    fmt.Println(output)
}