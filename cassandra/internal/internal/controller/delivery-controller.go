package controller

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/handler"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/service"
	"strconv"
)

type DeliveryController interface {
	handler.TransactionHandler
}

type deliveryControllerImpl struct {
	s service.DeliveryService
}

func NewDeliveryTransactionController(cassandraSession *common.CassandraSession) DeliveryController {
	return &deliveryControllerImpl{
		s: service.NewDeliveryService(cassandraSession),
	}
}

func (d *deliveryControllerImpl) HandleTransaction(cmd []string) {
	request := makeDeliveryRequest(cmd)
	d.s.ProcessDeliveryTransaction(request)
}

func makeDeliveryRequest(cmd []string) *model.DeliveryRequest {
	wId, _ := strconv.Atoi(cmd[1])
	carrierId, _ := strconv.Atoi(cmd[2])

	return &model.DeliveryRequest{
		WId:       wId,
		CarrierId: carrierId,
	}
}

func (d *deliveryControllerImpl) Close() error {
	panic("implement me")
}
