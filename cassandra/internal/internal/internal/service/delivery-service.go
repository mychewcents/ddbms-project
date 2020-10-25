package service

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/dao"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"io"
	"log"
)

type DeliveryService interface {
	ProcessDeliveryTransaction(request *model.DeliveryRequest) error
	io.Closer
}

type deliveryServiceImpl struct {
	o dao.OrderDao
	c dao.CustomerDao
}

func NewDeliveryService(cassandraSession *common.CassandraSession) DeliveryService {
	return &deliveryServiceImpl{
		o: dao.NewOrderDao(cassandraSession),
		c: dao.NewCustomerDao(cassandraSession),
	}
}

func (d *deliveryServiceImpl) ProcessDeliveryTransaction(request *model.DeliveryRequest) error {
	ch := make(chan bool, 10)
	for i := 1; i <= 10; i++ {
		go d.updateOldestOrderDelivery(request.WId, i, request.CarrierId, ch)
	}

	for i := 1; i <= 10; i++ {
		<-ch
	}
	return nil
}

func (d *deliveryServiceImpl) updateOldestOrderDelivery(oWId int, oDId int, oCarrierId int, ch chan bool) {
	ov := d.o.GetOldestUnDeliveredOrder(oWId, oDId)
	applied := d.o.UpdateOrderCAS(oWId, oDId, ov.OId, oCarrierId)

	if !applied {
		d.updateOldestOrderDelivery(oWId, oDId, oCarrierId, ch)
	} else {
		log.Println("CAS Failure updateOldestOrderDelivery")

		cCh := make(chan *table.CustomerTab)
		d.c.GetCustomerByKey(oWId, oDId, ov.OCId, cCh)
		ct := <-cCh
		d.c.UpdateCustomerDeliveryCAS(ct, ov.OOlTotalAmount)

		ch <- true
	}
}

func (d *deliveryServiceImpl) Close() error {
	panic("implement me")
}
