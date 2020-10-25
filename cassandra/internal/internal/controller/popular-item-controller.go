package controller

import (
	"fmt"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/handler"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/service"
)

type PopularItemController interface {
	handler.TransactionHandler
}

type popularItemControllerImpl struct {
	s service.PopularItemService
}

func NewPopularItemController(cassandraSession *common.CassandraSession) PopularItemController {
	return &popularItemControllerImpl{
		s: service.NewPopularItemService(cassandraSession),
	}
}

func (p *popularItemControllerImpl) HandleTransaction(cmd []string) {
	request := makePopularItemRequest(cmd)
	response, _ := p.s.ProcessPopularItemService(request)
	printPopularItemResponse(response)
}

func makePopularItemRequest(cmd []string) *model.PopularItemRequest {
	panic("implement me")
}

func printPopularItemResponse(r *model.PopularItemResponse) {
	fmt.Println(r)
}

func (p *popularItemControllerImpl) Close() error {
	panic("implement me")
}
