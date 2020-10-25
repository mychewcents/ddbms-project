package table

import (
	"github.com/gocql/gocql"
	"github.com/mitchellh/mapstructure"
)

const (
	OlWId       = "ol_w_id"
	OlDId       = "ol_d_id"
	OlOId       = "ol_o_id"
	OlQuantity  = "ol_quantity"
	OlNumber    = "ol_number"
	OlIId       = "ol_i_id"
	OlIName     = "ol_i_name"
	OlAmount    = "ol_amount"
	OlSupplyWId = "ol_supply_w_id"
	OlDistInfo  = "ol_dist_info"
)

/*
OrderLineTab maps to the order_line_tab in cassandra
PrimaryKey((ol_w_id, ol_d_id, ol_o_id), ol_quantity DESC, ol_number ASC)
*/
type OrderLineTab struct {
	OlWId       int        `mapstructure:"ol_w_id"`
	OlDId       int        `mapstructure:"ol_d_id"`
	OlOId       gocql.UUID `mapstructure:"ol_o_id"`
	OlQuantity  int        `mapstructure:"ol_quantity"`
	OlNumber    int        `mapstructure:"ol_number"`
	OlIId       int        `mapstructure:"ol_i_id"`
	OlIName     string     `mapstructure:"ol_i_name"`
	OlAmount    float32    `mapstructure:"ol_amount"`
	OlSupplyWId int        `mapstructure:"ol_supply_w_id"`
	OlDistInfo  string     `mapstructure:"ol_dist_info"`
}

type OrderLineItemQuantityTab struct {
}

func MakeOrderLineTab(columns map[string]interface{}) (*OrderLineTab, error) {
	var ot OrderLineTab

	if err := mapstructure.Decode(columns, &ot); err != nil {
		return nil, err
	}

	return &ot, nil
}
