package service

import (
	"github.com/gocql/gocql"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/dao"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"io"
)

type StockLevelService interface {
	ProcessStockLevelTransaction(request *model.StockLevelRequest) (*model.StockLevelResponse, error)
	io.Closer
}

type stockLevelServiceImpl struct {
	o  dao.OrderDao
	ol dao.OrderLineDao
	s  dao.StockDao
}

func NewStockLevelService(cassandraSession *common.CassandraSession) StockLevelService {
	return &stockLevelServiceImpl{
		o:  dao.NewOrderDao(cassandraSession),
		ol: dao.NewOrderLineDao(cassandraSession),
		s:  dao.NewStockDao(cassandraSession),
	}
}

func getUniqueItems(olts []*table.OrderLineTab) []int {
	iIdMap := make(map[int]bool)
	iIds := make([]int, 0)

	for _, olt := range olts {
		if !iIdMap[olt.OlIId] {
			iIdMap[olt.OlIId] = true
			iIds = append(iIds, olt.OlIId)
		}
	}

	return iIds
}

func (s *stockLevelServiceImpl) ProcessStockLevelTransaction(request *model.StockLevelRequest) (*model.StockLevelResponse, error) {
	ots := s.o.GetLatestNOrdersForDistrict(request.WId, request.DId, request.NoOfLastOrders)

	oIds := make([]gocql.UUID, request.NoOfLastOrders)
	for i, ot := range ots {
		oIds[i] = ot.OId
	}

	ch := make(chan []*table.OrderLineTab)
	s.ol.GetOrderLineItemListByKeys(request.WId, request.DId, oIds, ch)
	olts := <-ch

	iIds := getUniqueItems(olts)
	countCh := make(chan int)
	s.s.GetItemCountWithLowStock(request.WId, iIds, request.Threshold, countCh)

	return &model.StockLevelResponse{Count: <-countCh}, nil
}

func (s *stockLevelServiceImpl) Close() error {
	panic("implement me")
}
