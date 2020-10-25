package dao

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"log"
	"strings"
)

type StockDao interface {
	GetStockByKey(sWId int, sIId int, ch chan *table.StockTab)
	GetItemCountWithLowStock(sWId int, sIIds []int, sQuantity int, cCh chan int)
	UpdateStockCAS(stOld *table.StockTab, quantity int, isRemote bool, ch chan bool)
}

type stockDaoImpl struct {
	cassandraSession *common.CassandraSession
}

func NewStockDao(cassandraSession *common.CassandraSession) StockDao {
	return &stockDaoImpl{cassandraSession: cassandraSession}
}

func (s *stockDaoImpl) GetStockByKey(sWId int, sIId int, ch chan *table.StockTab) {
	query := s.cassandraSession.ReadSession.Query("SELECT * "+
		"from stock_tab "+
		"where s_w_id=? AND s_i_id=?", sWId, sIId)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		log.Fatalf("ERROR GetStockByKey error in query execution. sWId=%v, sIId=%v, err=%v\n", sWId, sIId, err)
		return
	}

	st, err := table.MakeStockTab(result)
	if err != nil {
		log.Fatalf("ERROR GetStockByKey error making stock. sWId=%v, sIId=%v, err=%v\n", sWId, sIId, err)
		return
	}

	ch <- st
}

func (s *stockDaoImpl) GetItemCountWithLowStock(sWId int, sIIds []int, sQuantity int, cCh chan int) {
	sIIdString := make([]string, len(sIIds))
	for i, sIId := range sIIds {
		sIIdString[i] = string(sIId)
	}

	query := s.cassandraSession.ReadSession.Query("SELECT count(*) "+
		"from stock_tab "+
		"where s_w_id=? AND s_i_id IN (?) AND s_quantity<?", sWId, strings.Join(sIIdString, ","), sQuantity)

	var count int
	if err := query.Scan(count); err != nil {
		log.Fatalf("ERROR GetItemCountWithLowStock error in query execution. sWId=%v, sIId=%v, err=%v\n", sWId, strings.Join(sIIdString, ","), err)
		return
	}

	cCh <- count
}

func (s *stockDaoImpl) UpdateStockCAS(stOld *table.StockTab, quantity int, isRemote bool, ch chan bool) {
	sQuantity := stOld.SQuantity - quantity
	if sQuantity < 10 {
		sQuantity = sQuantity + 100
	}
	sYtd := stOld.SYtd + quantity
	sOrderCnt := stOld.SOrderCnt + 1

	sRemoteCnt := stOld.SRemoteCnt
	if isRemote {
		sRemoteCnt++
	}

	query := s.cassandraSession.WriteSession.Query("UPDATE stock_tab "+
		"SET s_quantity=?, s_ytd=?, s_order_cnt=?, s_remote_cnt=? "+
		"WHERE s_w_id=? and s_i_id=? "+
		"IF s_quantity=? AND s_ytd=? AND s_order_cnt=? AND s_remote_cnt=?", sQuantity, sYtd, sOrderCnt, sRemoteCnt,
		stOld.SWId, stOld.SIId,
		stOld.SQuantity, stOld.SYtd, stOld.SOrderCnt, stOld.SRemoteCnt)

	applied, err := query.ScanCAS(&sQuantity, &sYtd, &sOrderCnt, &sRemoteCnt)
	if err != nil {
		log.Fatalf("ERROR UpdateStockCAS quering. err=%v\n", err)
		return
	}

	if !applied {
		log.Println("CAS Failure UpdateStockCAS")
		stOld.SQuantity = sQuantity
		stOld.SYtd = sYtd
		stOld.SOrderCnt = sOrderCnt
		stOld.SRemoteCnt = sRemoteCnt

		s.UpdateStockCAS(stOld, quantity, isRemote, ch)
	} else {
		ch <- true
	}
}
