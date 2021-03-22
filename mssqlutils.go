package mssqlutils

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ConnectOrDie func
func ConnectOrDie(server string, port string, user string, password string, database string, encrypt bool, trust bool) *sql.DB {

	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1024 {
		log.Fatalf("Failed creating SQL connection pool, illegal port: %s", port)
	}

	var db *sql.DB
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s;encrypt=%s;TrustServerCertificate=%s", server, user, password, portNumber, database, strconv.FormatBool(encrypt), strconv.FormatBool(trust))

	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatalf("Failed creating SQL connection pool: %s", err)
	}
	ctx := context.Background()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatalf("Connection ping failed: %s", err)
	}

	log.Printf("SQL connection pool created succesfully, encrypt: %v.", encrypt)
	return db
}

// ConnectOrFail func
func ConnectOrFail() error {
	return nil
}

func SQLMidnight24To00(s string) (string, error) {
	sqlTimePattern, err := regexp.Compile(`2\d{3}-\d{1,2}-\d{1,2} [\d]{1,2}:\d{1,2}:\d{1,2}.\d{1,3}`)
	if err == nil {
		match := sqlTimePattern.Find([]byte(s))
		matchString := string(match)
		if matchString == s {
			dateAndTime := strings.Split(matchString, " ")
			if len(dateAndTime) == 2 {
				dateString := dateAndTime[0]
				timeString := dateAndTime[1]
				timeSplitted := strings.Split(timeString, ":")
				if len(timeSplitted) == 3 {
					hour := timeSplitted[0]
					minutes := timeSplitted[1]
					seconds := timeSplitted[2]
					if hour == "24" {
						layout := "2006-01-02 15:04:05"
						dateString = dateString + " 00:00:00"
						goDate, err := time.Parse(layout, dateString)
						if err == nil {
							nextDate := goDate.AddDate(0, 0, 1)
							nextDateSplitted := strings.Split(nextDate.String(), " ")
							return nextDateSplitted[0] + " 00:" + minutes + ":" + seconds, nil
						}
						return s, errors.New("sql midnight cannot parse date")
					}
					return s, nil
				}
			}
		}
		return s, errors.New("sql midnight illegal value for sql time")
	}
	return s, err
}

func TransformString(length int, s string) string {
	tranformed := s
	if strings.Contains(s, "'") {
		tranformed = strings.ReplaceAll(s, "'", "''")
	}
	if len(tranformed) > length {
		return tranformed[:length]
	}
	return tranformed
}
