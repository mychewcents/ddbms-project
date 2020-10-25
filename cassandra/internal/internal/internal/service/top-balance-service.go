package service

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"io"
)

type TopBalanceService interface {
	ProcessTopBalanceTransaction(N int) (*model.TopBalanceResponse, error)
	io.Closer
}

type topBalanceServiceImpl struct {
}

func NewTopBalanceService(cassandraSession *common.CassandraSession) TopBalanceService {
	return &topBalanceServiceImpl{}
}

func (t *topBalanceServiceImpl) ProcessTopBalanceTransaction(N int) (*model.TopBalanceResponse, error) {
	panic("implement me")
}

func (t *topBalanceServiceImpl) Close() error {
	panic("implement me")
}
