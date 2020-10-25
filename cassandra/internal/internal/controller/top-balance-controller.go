package controller

import (
	"fmt"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/handler"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/service"
)

type TopBalanceController interface {
	handler.TransactionHandler
}

type topBalanceControllerImpl struct {
	s service.TopBalanceService
}

func NewTopBalanceController(cassandraSession *common.CassandraSession) TopBalanceController {
	return &topBalanceControllerImpl{
		s: service.NewTopBalanceService(cassandraSession),
	}
}

func (t *topBalanceControllerImpl) HandleTransaction(cmd []string) {
	N := makeTopBalanceRequest(cmd)
	response, _ := t.s.ProcessTopBalanceTransaction(N)
	printTopBalanceResponse(response)
}

func makeTopBalanceRequest(cmd []string) int {
	panic("")
}

func printTopBalanceResponse(r *model.TopBalanceResponse) {
	fmt.Println(r)
}

func (t *topBalanceControllerImpl) Close() error {
	panic("implement me")
}
