package service

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/dao"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"io"
	"strconv"
	"time"
)

type NewOrderService interface {
	ProcessNewOrderTransaction(request *model.NewOrderRequest) (*model.NewOrderResponse, error)
	io.Closer
}

type newOrderServiceImpl struct {
	c  dao.CustomerDao
	o  dao.OrderDao
	ol dao.OrderLineDao
	s  dao.StockDao
}

func NewNewOrderService(cassandraSession *common.CassandraSession) NewOrderService {
	return &newOrderServiceImpl{
		c:  dao.NewCustomerDao(cassandraSession),
		o:  dao.NewOrderDao(cassandraSession),
		ol: dao.NewOrderLineDao(cassandraSession),
		s:  dao.NewStockDao(cassandraSession),
	}
}

func (n *newOrderServiceImpl) ProcessNewOrderTransaction(request *model.NewOrderRequest) (*model.NewOrderResponse, error) {
	customerTab, stockTabMap := n.getCustomerAndStockInfo(request)

	oId := gocql.TimeUUID()
	orderTabList, totalAmount := makeOrderLineList(request, oId, stockTabMap)
	orderTab := makeOrderTab(request, oId, customerTab, totalAmount)

	n.updateInParallel(request, stockTabMap, orderTabList, orderTab)

	totalAmount = totalAmount * float64(1+customerTab.CDTax+customerTab.CWTax) * float64(1-customerTab.CDiscount)
	return makeNewOrderResponse(orderTab, orderTabList, customerTab, stockTabMap, totalAmount), nil
}

func (n *newOrderServiceImpl) updateInParallel(request *model.NewOrderRequest, stockTabMap map[int]map[int]*table.StockTab,
	orderTabList []*table.OrderLineTab, orderTab *table.OrderTab) {

	ch := make(chan bool, 3)
	go n.setStockTabNewMap(request, stockTabMap, ch)
	go n.ol.BatchInsertOrderLine(orderTabList, ch)
	go n.o.InsertOrder(orderTab, ch)

	<-ch
	<-ch
	<-ch
}

func makeOrderTab(request *model.NewOrderRequest, oId gocql.UUID, ct *table.CustomerTab, totalAmount float64) *table.OrderTab {
	isAllLocal := true
	for _, ol := range request.NewOrderLineList {
		if ol.OlSupplyWId != request.WId {
			isAllLocal = false
			break
		}
	}

	return &table.OrderTab{
		OWId:           request.WId,
		ODId:           request.DId,
		OId:            oId,
		OCId:           request.CId,
		OCName:         ct.CName,
		OCarrierId:     -1,
		OOlCount:       len(request.NewOrderLineList),
		OOlTotalAmount: totalAmount,
		OAllLocal:      isAllLocal,
		OEntryD:        time.Now(),
	}
}

func makeOrderLineList(request *model.NewOrderRequest, oId gocql.UUID, stMap map[int]map[int]*table.StockTab) ([]*table.OrderLineTab, float64) {
	otList := make([]*table.OrderLineTab, len(request.NewOrderLineList))
	totalAmount := 0.0

	for i, ol := range request.NewOrderLineList {
		st := stMap[ol.OlSupplyWId][ol.OlIId]
		itemAmount := float32(ol.OlQuantity) * st.SIPrice
		n, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", itemAmount), 64)
		itemAmount = float32(n)
		totalAmount += float64(itemAmount)

		ot := &table.OrderLineTab{
			OlWId:       request.WId,
			OlDId:       request.DId,
			OlOId:       oId,
			OlQuantity:  ol.OlQuantity,
			OlNumber:    i + 1,
			OlIId:       ol.OlIId,
			OlIName:     st.SIName,
			OlAmount:    itemAmount,
			OlSupplyWId: ol.OlSupplyWId,
			OlDistInfo:  st.GetSDist(request.DId),
		}

		otList[i] = ot
	}

	return otList, totalAmount
}

func (n *newOrderServiceImpl) setStockTabNewMap(request *model.NewOrderRequest, stMap map[int]map[int]*table.StockTab, chComplete chan bool) {
	ch := make(chan bool, len(request.NewOrderLineList))

	for _, ol := range request.NewOrderLineList {
		st := stMap[ol.OlSupplyWId][ol.OlIId]
		go n.s.UpdateStockCAS(st, ol.OlQuantity, !(ol.OlSupplyWId == request.WId), ch)
	}

	for range request.NewOrderLineList {
		<-ch
	}

	chComplete <- true
}

func (n *newOrderServiceImpl) getCustomerAndStockInfo(request *model.NewOrderRequest) (*table.CustomerTab, map[int]map[int]*table.StockTab) {
	customerTabCh := make(chan *table.CustomerTab)
	go n.c.GetCustomerByKey(request.WId, request.DId, request.CId, customerTabCh)

	stockTabListCh := make(chan *table.StockTab, len(request.NewOrderLineList))
	for _, ol := range request.NewOrderLineList {
		go n.s.GetStockByKey(ol.OlSupplyWId, ol.OlIId, stockTabListCh)
	}

	stockTabMap := make(map[int]map[int]*table.StockTab)
	for range request.NewOrderLineList {
		stockTab := <-stockTabListCh

		if stockTabMap[stockTab.SWId] == nil {
			stockTabMap[stockTab.SWId] = make(map[int]*table.StockTab)
		}
		stockTabMap[stockTab.SWId][stockTab.SIId] = stockTab
	}

	return <-customerTabCh, stockTabMap
}

func makeNewOrderResponse(ot *table.OrderTab, oltList []*table.OrderLineTab, customerTab *table.CustomerTab,
	stMap map[int]map[int]*table.StockTab, totalAmount float64) *model.NewOrderResponse {

	oliList := make([]*model.NewOrderLineInfo, len(oltList))

	for i, ol := range oltList {

		quantity := stMap[ol.OlSupplyWId][ol.OlIId].SQuantity - ol.OlQuantity
		if quantity < 10 {
			quantity = quantity + 100
		}

		oli := &model.NewOrderLineInfo{
			IId:         ol.OlIId,
			IName:       ol.OlIName,
			SupplierWId: ol.OlSupplyWId,
			Quantity:    ol.OlQuantity,
			OlAmount:    ol.OlAmount,
			SQuantity:   quantity,
		}

		oliList[i] = oli
	}

	return &model.NewOrderResponse{
		WId:                  ot.OWId,
		DId:                  ot.ODId,
		CId:                  ot.OCId,
		CCredit:              customerTab.CCredit,
		CDiscount:            customerTab.CDiscount,
		WTax:                 customerTab.CWTax,
		DTax:                 customerTab.CDTax,
		OId:                  ot.OId,
		OEntryD:              ot.OEntryD,
		NoOfItems:            len(oltList),
		TotalAmount:          totalAmount,
		NewOrderLineInfoList: oliList,
	}
}

func (n *newOrderServiceImpl) Close() error {
	panic("implement me")
}
