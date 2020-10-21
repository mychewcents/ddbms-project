package order_status
import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"github.com/cockroachdb/cockroach-go/crdb"
)

type orderItem struct {
	itemId, supplyWHNumber, quantity int
	itemAmount float64
}

func ProcessTransaction(db *sql.DB, warehouseId int, districtId int, customerId int) {
	// Create secondary index on (O_C_ID, O_ID DESC)
	// Add O_DELIVERY_D to ORDERS
	lastOrderQuery := fmt.Sprintf("SELECT O_ID, O_DELIVERY_D, O_ENTRY_D, O_CARRIER_ID FROM ORDERS_%d_%d WHERE O_C_ID=%d ORDER BY O_ID DESC LIMIT 1", 
		warehouseId, districtId, customerId)
	customerExistsQuery := fmt.Sprintf("SELECT C_FIRST, C_MIDDLE, C_LAST, C_BALANCE  FROM CUSTOMER WHERE C_W_ID=%d AND C_D_ID=%d AND C_ID=%d", warehouseId, districtId, customerId)	
	orderLinesQuery := "SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT FROM ORDER_LINE_%d_%d WHERE OL_O_ID=%d"
	
	var first, middle, last string
	var balance float64
	err := crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		if err := tx.QueryRow(customerExistsQuery).Scan(&first, &middle, &last, &balance); err != nil {
			return err
		}
		return nil
	})
	if err == sql.ErrNoRows {
		fmt.Println("Customer not found!")
		return
	}
	
	var orderLines []orderItem
	var lastOrderId int
	var carrierId sql.NullInt32
	var deliveryDate, entryDate sql.NullString
	err = crdb.ExecuteTx(context.Background(), db, nil, func(tx *sql.Tx) error {
		if err := tx.QueryRow(lastOrderQuery).Scan(&lastOrderId, &deliveryDate, &entryDate, &carrierId); err != nil {
			return err
		}
		rows, err := db.Query(fmt.Sprintf(orderLinesQuery, warehouseId, districtId, lastOrderId))
		if err != nil {
			return err
		}
		for rows.Next() {
			var orderLine orderItem
			if err := rows.Scan(&orderLine.itemId, &orderLine.supplyWHNumber, &orderLine.quantity, &orderLine.itemAmount); err != nil {
				return err
			}
			orderLines = append(orderLines, orderLine)
		}
		return nil
	})
	if err == sql.ErrNoRows {
		fmt.Println("No records found!")
		return
	}
	if err != nil {
		log.Fatal(err)
	}

	output := "Customer name: %s %s %s \nBalance: %f\nOrder: %d\nEntry date: %s\nCarrier: %s\n"
	
	var toPrintDeliveryDate string
	if !deliveryDate.Valid {
		toPrintDeliveryDate = "undelivered"
	} else {
		toPrintDeliveryDate = deliveryDate.String
	}

	for _, orderLine := range orderLines {

		output += fmt.Sprintf("Item number: %d\nSupply Warehouse: %d\nQuantity: %d\nTotal price: %f\nDelivery date: %s\n", 
			orderLine.itemId, orderLine.supplyWHNumber, orderLine.quantity, orderLine.itemAmount, toPrintDeliveryDate)
	}
	
	if carrierId.Valid && carrierId.Int32 != 0 {
		fmt.Println(fmt.Sprintf(output, first, middle, last, balance, lastOrderId, entryDate.String, strconv.Itoa(int(carrierId.Int32))))
	} else {
		fmt.Println(fmt.Sprintf(output, first, middle, last, balance, lastOrderId, entryDate.String, "null"))
	}
} 