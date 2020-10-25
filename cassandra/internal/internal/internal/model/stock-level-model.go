package model

type StockLevelRequest struct {
	WId            int
	DId            int
	Threshold      int
	NoOfLastOrders int
}

type StockLevelResponse struct {
	Count int
}
