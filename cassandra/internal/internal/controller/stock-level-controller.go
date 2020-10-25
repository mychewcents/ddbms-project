package controller

import (
	"fmt"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/handler"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/service"
)

type StockLevelController interface {
	handler.TransactionHandler
}

type stockLevelControllerImpl struct {
	s service.StockLevelService
}

func NewStockLevelController(cassandraSession *common.CassandraSession) StockLevelController {
	return &stockLevelControllerImpl{
		s: service.NewStockLevelService(cassandraSession),
	}
}

func (s *stockLevelControllerImpl) HandleTransaction(cmd []string) {
	request := makeStockLevelRequest(cmd)
	response, _ := s.s.ProcessStockLevelTransaction(request)
	printStockLevelResponse(response)
}

func printStockLevelResponse(r *model.StockLevelResponse) {
	fmt.Println(r)
}

func makeStockLevelRequest(cmd []string) *model.StockLevelRequest {
	panic("implement me")
}

func (s *stockLevelControllerImpl) Close() error {
	panic("implement me")
}
