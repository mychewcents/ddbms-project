package dao

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/table"
	"log"
)

type DistrictDao interface {
	GetDistrictByKey(dWId int, dId int, ch chan *table.DistrictTab)
	UpdateDistrictCAS(dtOld *table.DistrictTab, payment float64, ch chan bool)
}

type districtDaoImpl struct {
	cassandraSession *common.CassandraSession
}

func NewDistrictDao(cassandraSession *common.CassandraSession) DistrictDao {
	return &districtDaoImpl{cassandraSession: cassandraSession}
}

func (d *districtDaoImpl) GetDistrictByKey(dWId int, dId int, ch chan *table.DistrictTab) {
	query := d.cassandraSession.ReadSession.Query("SELECT * "+
		"from district_tab where d_w_id=? AND d_id=?", dWId, dId)

	result := make(map[string]interface{})
	if err := query.MapScan(result); err != nil {
		log.Fatalf("ERROR GetDistrictByKey error in query execution. wId=%v, dId=%v, err=%v\n", dWId, dId, err)
		return
	}

	dt, err := table.MakeDistrictTab(result)
	if err != nil {
		log.Fatalf("ERROR GetDistrictByKey error making district. wId=%v, dId=%v, err=%v\n", dWId, dId, err)
		return
	}

	ch <- dt
}

func (d *districtDaoImpl) UpdateDistrictCAS(dtOld *table.DistrictTab, payment float64, ch chan bool) {

	dytd := dtOld.DYtd + payment

	query := d.cassandraSession.WriteSession.Query("UPDATE district_tab "+
		"SET d_ytd=? "+
		"WHERE d_w_id=? and d_id=? "+
		"IF d_ytd=?", dytd,
		dtOld.DWId, dtOld.DId,
		dtOld.DYtd)

	applied, err := query.ScanCAS(&dytd)
	if err != nil {
		log.Fatalf("ERROR UpdateDistrictCAS quering. err=%v\n", err)
		return
	}

	if !applied {
		log.Println("CAS Failure UpdateDistrictCAS")
		dtOld.DYtd = dytd

		d.UpdateDistrictCAS(dtOld, payment, ch)
	} else {
		ch <- true
	}
}
