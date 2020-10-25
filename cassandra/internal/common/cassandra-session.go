package common

import "github.com/gocql/gocql"

type CassandraSession struct {
	ReadSession  *gocql.Session
	WriteSession *gocql.Session
}
