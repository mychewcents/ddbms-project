package controller

import (
	"fmt"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/handler"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/service"
)

type RelatedCustomerController interface {
	handler.TransactionHandler
}

type relatedCustomerControllerImpl struct {
	s service.RelatedCustomerService
}

func NewRelatedCustomerController(cassandraSession *common.CassandraSession) RelatedCustomerController {
	return &relatedCustomerControllerImpl{
		s: service.NewRelatedCustomerService(cassandraSession),
	}
}

func (r *relatedCustomerControllerImpl) HandleTransaction(cmd []string) {
	request := makeRelatedCustomerRequest(cmd)
	response, _ := r.s.ProcessRelatedCustomerTransaction(request)
	printRelatedCustomerResponse(response)
}

func makeRelatedCustomerRequest(cmd []string) *model.RelatedCustomerRequest {
	panic("implement me")
}

func printRelatedCustomerResponse(r *model.RelatedCustomerResponse) {
	fmt.Println(r)
}

func (r *relatedCustomerControllerImpl) Close() error {
	panic("implement me")
}
