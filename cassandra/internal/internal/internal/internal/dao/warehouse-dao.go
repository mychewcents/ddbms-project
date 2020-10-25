package dao

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"log"
)

type WarehouseDao interface {
	GetWarehouseByKey(wId int, ch chan *table.WarehouseTab)
	UpdateWarehouseCAS(wtOld *table.WarehouseTab, payment float64, ch chan bool)
}

type warehouseDaoImpl struct {
	cassandraSession *common.CassandraSession
}

func NewWarehouseDao(cassandraSession *common.CassandraSession) WarehouseDao {
	return &warehouseDaoImpl{cassandraSession: cassandraSession}
}

func (w *warehouseDaoImpl) GetWarehouseByKey(wId int, ch chan *table.WarehouseTab) {
	query := w.cassandraSession.ReadSession.Query("SELECT * "+
		"from warehouse_tab where w_id=?", wId)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		log.Fatalf("ERROR GetWarehouseByKey error in query execution. wId=%v, err=%v\n", wId, err)
		return
	}

	wt, err := table.MakeWarehouseTab(result)
	if err != nil {
		log.Fatalf("ERROR GetWarehouseByKey error making warehouse. wId=%v, err=%v\n", wId, err)
		return
	}

	ch <- wt
}

func (w *warehouseDaoImpl) UpdateWarehouseCAS(wtOld *table.WarehouseTab, payment float64, ch chan bool) {

	wYtd := wtOld.WYtd + payment

	query := w.cassandraSession.WriteSession.Query("UPDATE warehouse_tab "+
		"SET w_ytd=? "+
		"WHERE w_id=? "+
		"IF w_ytd=?", wYtd,
		wtOld.WId,
		wtOld.WYtd)

	applied, err := query.ScanCAS(&wYtd)
	if err != nil {
		log.Fatalf("ERROR UpdateWarehouseCAS quering. err=%v\n", err)
		return
	}

	if !applied {
		log.Println("CAS Failure UpdateWarehouseCAS")
		wtOld.WYtd = wYtd

		w.UpdateWarehouseCAS(wtOld, payment, ch)
	} else {
		ch <- true
	}
}
