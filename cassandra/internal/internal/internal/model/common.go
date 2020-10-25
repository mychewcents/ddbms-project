package model

import (
	"github.com/mychewcents/ddbms-project/cassandra/internal/internal/internal/internal/datamodel/udt"
)

type Name struct {
	FirstName  string
	MiddleName string
	LastName   string
}

type Address struct {
	Street1 string
	Street2 string
	City    string
	State   string
	Zip     string
}

func NameModelFromUDT(n *udt.Name) *Name {
	return &Name{
		FirstName:  n.FirstName,
		MiddleName: n.MiddleName,
		LastName:   n.LastName,
	}
}

func AddressModelFromUDT(a *udt.Address) *Address {
	return &Address{
		Street1: a.Street1,
		Street2: a.Street2,
		City:    a.City,
		State:   a.State,
		Zip:     a.Zip,
	}
}
