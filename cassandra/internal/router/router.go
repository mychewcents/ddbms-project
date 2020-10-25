package router

import (
	"bufio"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/controller"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/handler"
	"strings"
)

type TransactionRouter interface {
	HandleCommand(command string)
}

type transactionRouterImpl struct {
	handlers map[string]handler.TransactionHandler
}

func NewTransactionRouter(cassandraSession *common.CassandraSession, reader *bufio.Reader) TransactionRouter {
	router := transactionRouterImpl{
		handlers: make(map[string]handler.TransactionHandler, 0),
	}
	router.registerHandlers(cassandraSession, reader)
	return &router
}

func (t *transactionRouterImpl) HandleCommand(command string) {
	commandSplit := strings.Split(strings.Trim(command, "\n"), ",")
	t.handlers[commandSplit[0]].HandleTransaction(commandSplit)
}

func (t *transactionRouterImpl) registerHandlers(cassandraSession *common.CassandraSession, reader *bufio.Reader) {
	t.handlers["N"] = controller.NewNewOrderTransactionController(cassandraSession, reader)
	t.handlers["P"] = controller.NewPaymentController(cassandraSession)
	t.handlers["D"] = controller.NewDeliveryTransactionController(cassandraSession)
	t.handlers["O"] = controller.NewOrderStatusTransactionController(cassandraSession)
	t.handlers["S"] = controller.NewStockLevelController(cassandraSession)
	t.handlers["I"] = controller.NewPopularItemController(cassandraSession)
	t.handlers["T"] = controller.NewTopBalanceController(cassandraSession)
	t.handlers["R"] = controller.NewRelatedCustomerController(cassandraSession)
}
