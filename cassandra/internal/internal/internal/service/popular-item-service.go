package service

import (
	"github.com/gocql/gocql"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/dao"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"io"
)

type PopularItemService interface {
	ProcessPopularItemService(request *model.PopularItemRequest) (*model.PopularItemResponse, error)
	io.Closer
}

type popularItemServiceImpl struct {
	o  dao.OrderDao
	ol dao.OrderLineDao
}

func NewPopularItemService(cassandraSession *common.CassandraSession) PopularItemService {
	return &popularItemServiceImpl{
		o:  dao.NewOrderDao(cassandraSession),
		ol: dao.NewOrderLineDao(cassandraSession),
	}
}

func (p *popularItemServiceImpl) ProcessPopularItemService(request *model.PopularItemRequest) (*model.PopularItemResponse, error) {
	ots := p.o.GetLatestNOrdersForDistrict(request.WId, request.DId, request.NoOfLastOrders)

	oIds := make([]gocql.UUID, request.NoOfLastOrders)
	for i, ot := range ots {
		oIds[i] = ot.OId
	}

	ch := make(chan []*table.OrderLineTab)
	p.ol.GetOrderLineItemListByKeys(request.WId, request.DId, oIds, ch)
	olts := <-ch

	orderToPopularItemsMap, popularItemsNameMap, itemsToOrderMap := getPopularItemInfo(olts)

	response := &model.PopularItemResponse{
		WId:            request.WId,
		DId:            request.DId,
		NoOfLastOrders: request.NoOfLastOrders,
	}

	for _, ot := range ots {
		response.OrderItemInfoList = append(response.OrderItemInfoList, &model.OrderItemInfo{
			OId:     ot.OId,
			OEntryD: ot.OEntryD,
			CName: &model.Name{
				FirstName:  ot.OCName.FirstName,
				MiddleName: ot.OCName.MiddleName,
				LastName:   ot.OCName.LastName,
			},
			PopularItemInfoList: orderToPopularItemsMap[ot.OId],
		})
	}

	for pi, pin := range popularItemsNameMap {
		response.PopularItemStatList = append(response.PopularItemStatList, &model.PopularItemStat{
			IName:           pin,
			OrderPercentage: float32(len(itemsToOrderMap[pi])) / float32(request.NoOfLastOrders) * 100.0,
		})
	}

	return response, nil
}

func getPopularItemInfo(olts []*table.OrderLineTab) (map[gocql.UUID][]*model.PopularItemInfo, map[int]string, map[int][]gocql.UUID) {
	orderToPopularItemsMap := make(map[gocql.UUID][]*model.PopularItemInfo)
	popularItemsNameMap := make(map[int]string)
	itemsToOrderMap := make(map[int][]gocql.UUID)

	var curOrderId gocql.UUID
	curHighestQuantity := 0

	for _, olt := range olts {
		if olt.OlOId != curOrderId {
			curOrderId = olt.OlOId
			curHighestQuantity = olt.OlQuantity
		}

		if olt.OlQuantity == curHighestQuantity {
			orderToPopularItemsMap[curOrderId] = append(orderToPopularItemsMap[curOrderId], &model.PopularItemInfo{
				IName:      olt.OlIName,
				OlQuantity: olt.OlQuantity,
			})
			popularItemsNameMap[olt.OlIId] = olt.OlIName
		}
		itemsToOrderMap[olt.OlIId] = append(itemsToOrderMap[olt.OlIId], olt.OlOId)
	}

	return orderToPopularItemsMap, popularItemsNameMap, itemsToOrderMap
}

func (p *popularItemServiceImpl) Close() error {
	panic("implement me")
}
