package service

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/dao"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/view"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"io"
)

type OrderStatusService interface {
	ProcessOrderStatusTransaction(request *model.OrderStatusRequest) (*model.OrderStatusResponse, error)
	io.Closer
}

type orderStatusServiceImpl struct {
	o  dao.OrderDao
	c  dao.CustomerDao
	ol dao.OrderLineDao
}

func NewOrderStatusService(cassandraSession *common.CassandraSession) OrderStatusService {
	return &orderStatusServiceImpl{
		o:  dao.NewOrderDao(cassandraSession),
		c:  dao.NewCustomerDao(cassandraSession),
		ol: dao.NewOrderLineDao(cassandraSession),
	}
}

func (o *orderStatusServiceImpl) ProcessOrderStatusTransaction(request *model.OrderStatusRequest) (*model.OrderStatusResponse, error) {
	chO := make(chan *view.OrderByCustomerView)
	chC := make(chan *table.CustomerTab)
	chOl := make(chan []*table.OrderLineTab)

	o.o.GetCustomerLatestOrder(request.CWId, request.CDId, request.CWId, chO)
	o.c.GetCustomerByKey(request.CWId, request.CDId, request.CId, chC)

	ov := <-chO
	o.ol.GetOrderLineListByKey(request.CWId, request.CDId, ov.OId, chOl)
	ct := <-chC
	olt := <-chOl

	olS := make([]*model.OrderLineStatus, len(olt))
	for i, ol := range olt {
		olS[i] = &model.OrderLineStatus{
			OlIId:       ol.OlIId,
			OlSupplyWId: ol.OlSupplyWId,
			OlQuantity:  ol.OlQuantity,
			OlAmount:    ol.OlAmount,
			OlDeliveryD: ov.OlDeliveryD,
		}
	}

	return &model.OrderStatusResponse{
		CName:               model.NameModelFromUDT(&ct.CName),
		OId:                 ov.OId,
		OEntryD:             ov.OEntryD,
		OCarrierId:          ov.OCarrierId,
		OrderLineStatusList: olS,
	}, nil
}

func (o *orderStatusServiceImpl) Close() error {
	panic("implement me")
}
