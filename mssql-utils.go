package mssql-utils

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
)

// ConnectOrDie func
func ConnectOrDie(server string, port int8, user string, password string, database string, encrypt bool, trust bool) *sql.DB {

	var db *sql.DB
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;encrypt=%s;TrustServerCertificate=%s", server, user, password, port, database, strconv.FormatBool(encrypt), strconv.FormatBool(trust))

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatalf("Failed creating SQL connection pool: %s", err)
	}
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatalf("Connection ping failed: %s", err)
	}
	log.Println("SQL connection pool created succesfully.")
	return db
}