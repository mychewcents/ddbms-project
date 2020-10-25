package dao

import (
	"github.com/gocql/gocql"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"log"
	"strings"
)

type OrderLineDao interface {
	BatchInsertOrderLine(oltList []*table.OrderLineTab, chComplete chan bool)
	GetOrderLineListByKey(oWId int, oDId int, oId gocql.UUID, ch chan []*table.OrderLineTab)
	GetOrderLineItemListByKeys(oWId int, oDId int, oIds []gocql.UUID, ch chan []*table.OrderLineTab)
}

type orderLineDaoImpl struct {
	cassandraSession *common.CassandraSession
}

func NewOrderLineDao(cassandraSession *common.CassandraSession) OrderLineDao {
	return &orderLineDaoImpl{cassandraSession: cassandraSession}
}

func (o *orderLineDaoImpl) BatchInsertOrderLine(oltList []*table.OrderLineTab, chComplete chan bool) {
	batch := o.cassandraSession.WriteSession.NewBatch(gocql.LoggedBatch)
	stmt := "INSERT INTO order_line_tab (ol_w_id, ol_d_id, ol_o_id, ol_quantity, ol_number, ol_i_id, ol_i_name, ol_amount, ol_supply_w_id, ol_dist_info) VALUES (?,?,?,?,?,?,?,?,?,?)"

	for _, ol := range oltList {
		batch.Query(stmt, ol.OlWId, ol.OlDId, ol.OlOId, ol.OlQuantity, ol.OlNumber, ol.OlIId, ol.OlIName, ol.OlAmount, ol.OlSupplyWId, ol.OlDistInfo)
	}

	err := o.cassandraSession.WriteSession.ExecuteBatch(batch)
	if err != nil {
		log.Fatalf("ERROR BatchInsertNewOrderLine Error Executing batch err=%v", err)
	}

	chComplete <- true
}

func (o *orderLineDaoImpl) GetOrderLineListByKey(oWId int, oDId int, oId gocql.UUID, ch chan []*table.OrderLineTab) {
	query := o.cassandraSession.ReadSession.Query("SELECT * "+
		"from order_line_tab "+
		"where o_w_id=? AND o_d_id=? AND o_id=?", oWId, oDId, oId)

	olts := make([]*table.OrderLineTab, 0)

	iter := query.Iter()
	defer iter.Close()

	for result := make(map[string]interface{}); iter.MapScan(result); result = make(map[string]interface{}) {
		ot, err := table.MakeOrderLineTab(result)
		if err != nil {
			log.Fatalf("ERROR GetOrderLineListByKey error making orderLine. oWId=%v, oDId=%v, oId=%v, err=%v\n", oWId, oDId, oId, err)
			return
		}
		olts = append(olts, ot)
	}

	ch <- olts
}

func (o *orderLineDaoImpl) GetOrderLineItemListByKeys(oWId int, oDId int, oIds []gocql.UUID, ch chan []*table.OrderLineTab) {
	oIdString := make([]string, len(oIds))
	for i, oId := range oIds {
		oIdString[i] = oId.String()
	}

	query := o.cassandraSession.ReadSession.Query("SELECT * "+
		"from order_line_tab "+
		"where o_w_id=? AND o_d_id=? AND o_id IN (?)", oWId, oDId, strings.Join(oIdString, ","))

	olts := make([]*table.OrderLineTab, 0)

	iter := query.Iter()
	defer iter.Close()

	for result := make(map[string]interface{}); iter.MapScan(result); result = make(map[string]interface{}) {
		ot, err := table.MakeOrderLineTab(result)
		if err != nil {
			log.Fatalf("ERROR GetOrderLineItemListByKeys error making orderLine. oWId=%v, oDId=%v, oIds=%v, err=%v\n", oWId, oDId, strings.Join(oIdString, ","), err)
			return
		}
		olts = append(olts, ot)
	}

	ch <- olts
}
