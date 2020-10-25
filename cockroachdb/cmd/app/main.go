package main

import (
	"bufio"
	"database/sql"
	"flag"
	"os"
	"strings"

	caller "github.com/mychewcents/ddbms-project/cockroachdb/internal"
	"github.com/mychewcents/ddbms-project/cockroachdb/internal/cdbconn"
)

var db *sql.DB

var (
	connPtr     = flag.String("host", "localhost", "URL / IP of the DB Server")
	portPtr     = flag.Int("port", 26257, "Port to contact the server's CDB Service")
	dbPtr       = flag.String("database", "defaultdb", "Database to connect")
	usernamePtr = flag.String("username", "root", "Username to connect with")
)

func init() {
	var err error
	flag.Parse()

	db, err = cdbconn.CreateConnection(*connPtr, *portPtr, *dbPtr, *usernamePtr)
	if err != nil {
		panic(err)
	}
}

func main() {
	var txArgs []string

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		txArgs = strings.Split(scanner.Text(), ",")

		caller.ProcessRequest(db, scanner, txArgs)
	}
}
