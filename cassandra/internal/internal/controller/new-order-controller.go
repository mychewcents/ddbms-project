package controller

import (
	"bufio"
	"fmt"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/handler"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/service"
	"strconv"
	"strings"
)

type NewOrderController interface {
	handler.TransactionHandler
}

type newOrderControllerImpl struct {
	s service.NewOrderService
	r *bufio.Reader
}

func NewNewOrderTransactionController(cassandraSession *common.CassandraSession, reader *bufio.Reader) NewOrderController {
	return &newOrderControllerImpl{
		s: service.NewNewOrderService(cassandraSession),
		r: reader,
	}
}

func (n *newOrderControllerImpl) HandleTransaction(cmd []string) {
	request := makeNewOrderRequest(cmd, n.r)
	resp, _ := n.s.ProcessNewOrderTransaction(request)
	printNewOrderResponse(resp)
}

func makeNewOrderRequest(cmd []string, r *bufio.Reader) *model.NewOrderRequest {
	cId, _ := strconv.Atoi(cmd[1])
	wId, _ := strconv.Atoi(cmd[2])
	dId, _ := strconv.Atoi(cmd[3])
	m, _ := strconv.Atoi(cmd[4])

	return &model.NewOrderRequest{
		WId:              wId,
		DId:              dId,
		CId:              cId,
		NewOrderLineList: makeNewOrderLineList(m, r),
	}
}

func makeNewOrderLineList(m int, r *bufio.Reader) []*model.NewOrderLine {
	newOrderLineList := make([]*model.NewOrderLine, m)

	for i := 0; i < m; i++ {
		text, _ := r.ReadString('\n')
		orderLineSplit := strings.Split(strings.Trim(text, "\n"), ",")

		olIId, _ := strconv.Atoi(orderLineSplit[0])
		olSupplyWId, _ := strconv.Atoi(orderLineSplit[1])
		olQuantity, _ := strconv.Atoi(orderLineSplit[2])

		newOrderLine := &model.NewOrderLine{
			OlIId:       olIId,
			OlSupplyWId: olSupplyWId,
			OlQuantity:  olQuantity,
		}

		newOrderLineList[i] = newOrderLine
	}

	return newOrderLineList
}

func printNewOrderResponse(r *model.NewOrderResponse) {
	fmt.Println(r)
}

func (n *newOrderControllerImpl) Close() error {
	panic("implement me")
}
