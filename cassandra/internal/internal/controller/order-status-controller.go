package controller

import (
	"fmt"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/handler"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/service"
)

type OrderStatusController interface {
	handler.TransactionHandler
}

type orderStatusControllerImpl struct {
	s service.OrderStatusService
}

func NewOrderStatusTransactionController(cassandraSession *common.CassandraSession) OrderStatusController {
	return &orderStatusControllerImpl{
		s: service.NewOrderStatusService(cassandraSession),
	}
}

func (n *orderStatusControllerImpl) HandleTransaction(cmd []string) {
	request := makeOrderStatusRequest(cmd)
	response, _ := n.s.ProcessOrderStatusTransaction(request)
	printOrderStatusResponse(response)
}

func makeOrderStatusRequest(cmd []string) *model.OrderStatusRequest {
	panic("implement me")
}

func printOrderStatusResponse(r *model.OrderStatusResponse) {
	fmt.Println(r)
}

func (n *orderStatusControllerImpl) Close() error {
	panic("implement me")
}
