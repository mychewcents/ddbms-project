package view

import (
	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"
)

/*
OrderByCarrierView maps to the order_by_carrier_view in cassandra
PrimaryKey((o_w_id, o_d_id), o_carrier_id ASC, o_id ASC)
*/
type OrderByCarrierView struct {
	OWId           int        `mapstructure:"o_w_id"`
	ODId           int        `mapstructure:"o_d_id"`
	OCarrierId     int        `mapstructure:"o_carrier_id"`
	OId            gocql.UUID `mapstructure:"o_id"`
	OCId           int        `mapstructure:"o_c_id"`
	OOlTotalAmount float64    `mapstructure:"o_ol_total_amount"`
}

func MakeOrderByCarrierView(columns map[string]interface{}) (*OrderByCarrierView, error) {
	var ot OrderByCarrierView

	if err := mapstructure.Decode(columns, &ot); err != nil {
		return nil, err
	}

	return &ot, nil
}
