package cassandra_client

import (
	"bufio"
	"encoding/xml"
	"github.com/gocql/gocql"
	"github.com/mychewcents/ddbms-project/cassandra/internal/common"
	"github.com/mychewcents/ddbms-project/cassandra/internal/config"
	"github.com/mychewcents/ddbms-project/cassandra/internal/router"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func Start() {
	cassandraSession := makeCassandraSession()
	reader := bufio.NewReader(os.Stdin)

	r := router.NewTransactionRouter(cassandraSession, reader)

	text, _ := reader.ReadString('\n')

	for text != "" {
		r.HandleCommand(text)
		text, _ = reader.ReadString('\n')
	}
}

func makeCassandraSession() *common.CassandraSession {
	cassandraConfig := makeCassandraConfig()

	readCluster := gocql.NewCluster(cassandraConfig.Hosts...)
	readCluster.Keyspace = cassandraConfig.Keyspace
	if strings.ToUpper(cassandraConfig.ReadConsistency) == "ONE" {
		readCluster.Consistency = gocql.One
	} else {
		readCluster.Consistency = gocql.Quorum
	}
	readSession, _ := readCluster.CreateSession()

	writeCluster := gocql.NewCluster(cassandraConfig.Hosts...)
	writeCluster.Keyspace = cassandraConfig.Keyspace
	if strings.ToUpper(cassandraConfig.WriteConsistency) == "ONE" {
		writeCluster.Consistency = gocql.One
	} else {
		writeCluster.Consistency = gocql.Quorum
	}
	writeSession, _ := writeCluster.CreateSession()

	return &common.CassandraSession{
		ReadSession:  readSession,
		WriteSession: writeSession,
	}
}

func makeCassandraConfig() *config.CassandraConfig {
	xmlFile, err := os.Open("configs/cassandra-config.xml")
	if err != nil {
		log.Fatal(err)
	}
	defer xmlFile.Close()

	byteValue, _ := ioutil.ReadAll(xmlFile)

	var cassandraConfig config.CassandraConfig
	err = xml.Unmarshal(byteValue, &cassandraConfig)
	if err != nil {
		log.Fatal(err)
	}

	return &cassandraConfig
}
