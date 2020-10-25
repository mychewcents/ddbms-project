package view

import (
	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"
	"time"
)

/*
OrderByCustomerView maps to the order_by_customer_view in cassandra
PrimaryKey((o_w_id, o_d_id), o_c_id ASC, o_id DESC)
*/
type OrderByCustomerView struct {
	OWId        int        `mapstructure:"o_w_id"`
	ODId        int        `mapstructure:"o_d_id"`
	OCId        int        `mapstructure:"o_c_id"`
	OId         gocql.UUID `mapstructure:"o_id"`
	OEntryD     time.Time  `mapstructure:"o_entry_d"`
	OCarrierId  int        `mapstructure:"o_carrier_id"`
	OlDeliveryD time.Time  `mapstructure:"ol_delivery_d"`
}

func MakeOrderByCustomerView(columns map[string]interface{}) (*OrderByCustomerView, error) {
	var ot OrderByCustomerView

	if err := mapstructure.Decode(columns, &ot); err != nil {
		return nil, err
	}

	return &ot, nil
}
