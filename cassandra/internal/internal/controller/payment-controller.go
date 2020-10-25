package controller

import (
	"fmt"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/handler"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/model"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/service"
	"strconv"
)

type PaymentController interface {
	handler.TransactionHandler
}

type paymentControllerImpl struct {
	s service.PaymentService
}

func NewPaymentController(cassandraSession *common.CassandraSession) PaymentController {
	return &paymentControllerImpl{
		s: service.NewPaymentService(cassandraSession),
	}
}

func (p *paymentControllerImpl) HandleTransaction(cmd []string) {
	request := makePaymentRequest(cmd)
	response, _ := p.s.ProcessPaymentTransaction(request)
	printPaymentResponse(response)
}

func makePaymentRequest(cmd []string) *model.PaymentRequest {
	cWId, _ := strconv.Atoi(cmd[1])
	cDId, _ := strconv.Atoi(cmd[2])
	cId, _ := strconv.Atoi(cmd[3])
	payment, _ := strconv.ParseFloat(cmd[4], 64)

	return &model.PaymentRequest{
		CWId:    cWId,
		CDId:    cDId,
		CId:     cId,
		Payment: payment,
	}
}

func printPaymentResponse(r *model.PaymentResponse) {
	fmt.Println(r)
}

func (p *paymentControllerImpl) Close() error {
	panic("implement me")
}
