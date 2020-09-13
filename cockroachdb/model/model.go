package model

import "fmt"
type NewOrderInput struct {
	c_id, d_id, w_id int64
	m int
	ol_i_id, ol_supply_w_id []int64
	ol_quantity []float32
}

func (inp *NewOrderInput) InitNewOrderInput(c_id int64, d_id int64, w_id int64, m int, ol_i_id []int64, 
	ol_supply_w_id []int64, ol_quantity []float32) {
	inp.c_id = c_id
	inp.d_id = d_id
	inp.w_id = w_id
	inp.m = m
	inp.ol_quantity = ol_quantity
	inp.ol_i_id = ol_i_id
	inp.ol_supply_w_id = ol_supply_w_id
}

func (inp NewOrderInput) PrintNewOrderInput() {
	fmt.Printf("c_id: %d, d_id: %d, w_id: %d, m: %d\n", inp.c_id, inp.d_id, inp.w_id, inp.m)
	for i := 0; i < inp.m; i++ {
		fmt.Printf("ol_id: %d, ol_supply_w_id: %d, ol_quantity: %f\n", inp.ol_i_id[i], inp.ol_supply_w_id[i], inp.ol_quantity[i])
	}
}

type TransactionResult struct {
	// Total number of transactions processed
	// Total elapsed time for processing the transactions (in seconds)
	// Transaction throughput (number of transactions processed per second)
	// Average transaction latency (in ms)
	// Median transaction latency (in ms)
	// 95th percentile transaction latency (in ms)
	// 99th percentile transaction latency (in ms)
}

type PaymentInput struct {
	c_w_id, c_d_id, c_id int64
	payment float32
}

func (inp *PaymentInput) InitPaymentInput(c_w_id int64, c_d_id int64, c_id int64, payment float32) {
	inp.c_w_id = c_w_id
	inp.c_d_id = c_d_id
	inp.c_id = c_id
	inp.payment = payment
}

func (inp PaymentInput) PrintPaymentInput() {
	fmt.Println(inp.c_w_id, inp.c_d_id, inp.c_id, inp.payment)
}