package service

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/dao"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"io"
)

type PaymentService interface {
	ProcessPaymentTransaction(request *model.PaymentRequest) (*model.PaymentResponse, error)
	io.Closer
}

type paymentServiceImpl struct {
	w dao.WarehouseDao
	d dao.DistrictDao
	c dao.CustomerDao
}

func NewPaymentService(cassandraSession *common.CassandraSession) PaymentService {
	return &paymentServiceImpl{
		w: dao.NewWarehouseDao(cassandraSession),
		d: dao.NewDistrictDao(cassandraSession),
		c: dao.NewCustomerDao(cassandraSession),
	}
}

func (p *paymentServiceImpl) ProcessPaymentTransaction(request *model.PaymentRequest) (*model.PaymentResponse, error) {
	warehouseTab, districtTab, customerTab := p.getWarehouseDistrictAndCustomerInfo(request)

	p.updateWarehouseDistrictAndCustomer(warehouseTab, districtTab, customerTab, request.Payment)

	return makePaymentResponse(warehouseTab, districtTab, customerTab, request.Payment), nil
}

func (p *paymentServiceImpl) getWarehouseDistrictAndCustomerInfo(request *model.PaymentRequest) (*table.WarehouseTab, *table.DistrictTab, *table.CustomerTab) {
	chW := make(chan *table.WarehouseTab)
	chD := make(chan *table.DistrictTab)
	chC := make(chan *table.CustomerTab)

	go p.w.GetWarehouseByKey(request.CWId, chW)
	go p.d.GetDistrictByKey(request.CWId, request.CDId, chD)
	go p.c.GetCustomerByKey(request.CWId, request.CDId, request.CId, chC)

	return <-chW, <-chD, <-chC
}

func (p *paymentServiceImpl) updateWarehouseDistrictAndCustomer(wt *table.WarehouseTab, dt *table.DistrictTab, ct *table.CustomerTab, payment float64) {
	ch := make(chan bool, 3)
	go p.w.UpdateWarehouseCAS(wt, payment, ch)
	go p.d.UpdateDistrictCAS(dt, payment, ch)
	go p.c.UpdateCustomerPaymentCAS(ct, payment, ch)

	<-ch
	<-ch
	<-ch
}

func makePaymentResponse(wt *table.WarehouseTab, dt *table.DistrictTab, ct *table.CustomerTab, payment float64) *model.PaymentResponse {
	return &model.PaymentResponse{
		CWId:       ct.CWId,
		CDId:       ct.CDId,
		CId:        ct.CId,
		CName:      model.NameModelFromUDT(&ct.CName),
		CAddress:   model.AddressModelFromUDT(&ct.CAddress),
		CPhone:     ct.CPhone,
		CSince:     ct.CSince,
		CCredit:    ct.CCredit,
		CCreditLim: ct.CCreditLim,
		CDiscount:  ct.CDiscount,
		CBalance:   ct.CBalance,
		WAddress:   model.AddressModelFromUDT(&wt.WAddress),
		DAddress:   model.AddressModelFromUDT(&dt.DAddress),
		Payment:    payment,
	}
}

func (p *paymentServiceImpl) Close() error {
	panic("implement me")
}
