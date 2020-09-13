package executors
import (
	"fmt"
	"github.com/mychewcents/ddbms-project/cockroachdb/model"
)

func NewOrderExecutor(input model.NewOrderInput)  {
	fmt.Println("Executing new order transaction...")
	// ...
	fmt.Println("New order transaction completed...")	
}

func PaymentExecutor(input model.PaymentInput) {
	fmt.Println("Executing payment transaction...")
	// ...
	fmt.Println("Payment transaction completed...")
}