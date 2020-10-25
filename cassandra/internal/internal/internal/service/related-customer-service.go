package service

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"io"
)

type RelatedCustomerService interface {
	ProcessRelatedCustomerTransaction(request *model.RelatedCustomerRequest) (*model.RelatedCustomerResponse, error)
	io.Closer
}

type relatedCustomerServiceImpl struct {
}

func NewRelatedCustomerService(cassandraSession *common.CassandraSession) RelatedCustomerService {
	return &relatedCustomerServiceImpl{}
}

func (r *relatedCustomerServiceImpl) ProcessRelatedCustomerTransaction(request *model.RelatedCustomerRequest) (*model.RelatedCustomerResponse, error) {
	panic("implement me")
}

func (r *relatedCustomerServiceImpl) Close() error {
	panic("implement me")
}
