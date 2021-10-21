package mysql_session

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"sync"
	"time"
)

var once sync.Once
var instanceMysql **sql.DB

func GetMysqlConnect() *sql.DB {
	once.Do(func() {
		db, err := sql.Open(`mysql`, `root:secret@tcp(127.0.0.1:4308)/test?parseTime=true`)
		if err != nil {
			log.Fatal(err.Error())
		}
		db.SetConnMaxLifetime(time.Minute * 3)
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(10)
		
		instanceMysql = &db
	})

	err := (*instanceMysql).Ping()
	if err != nil {
		log.Fatal(err.Error())
	}

	return *instanceMysql
}

func CloseMysqlConnect() {
	(*instanceMysql).Close()
}