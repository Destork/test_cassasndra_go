package read_record

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"math/rand"
	"test_cassandra_go/sessions/cassandra_session"
	"test_cassandra_go/sessions/mysql_session"
	"test_cassandra_go/transaction"
	"time"
)

func getMysqlRecords() {
	sql := `
SELECT
	transaction__uuid,
	user__id,
	transaction__create_time,
	source_wallet__id,
	target_wallet__id,
	transaction__amount,
	transaction__wallet_previous_balance,
	transaction__rate_exchange,
	transaction__system_type,
	transaction__system_id
FROM transaction
WHERE user__id = ? AND transaction__create_time > ?`

	scanner, err := mysql_session.GetMysqlConnect().Query(sql,
		6,
		time.Date(2021, 10, 18, 10, 12, 0, 0, time.Now().Location()))

	if err != nil {
		log.Fatal(err.Error())
	}

	defer func() {
		err := scanner.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	var result []transaction.Transaction

	for scanner.Next() {
		row, err := transaction.UnmarshalMysqlTransaction(scanner)

		if err != nil {
			log.Fatal(err.Error())
		}

		result = append(result, row)
	}

	for _, tr := range result {
		fmt.Println(tr.TransactionUuid, tr.CreateTime, tr.Amount)
	}
}

func getCassandraRecords() {
	cql := `
SELECT
	transaction__uuid,
	user__id,
	transaction__create_time,
	source_wallet__id,
	target_wallet__id,
	transaction__amount,
	transaction__wallet_previous_balance,
	transaction__rate_exchange,
	transaction__system_type,
	transaction__system_id
FROM transaction
WHERE user__id = ? AND transaction__uuid > ?`

	session := cassandra_session.GetCassandraSession()

	var result []transaction.Transaction

	start := time.Now()

	scanner := session.Query(cql,
		int64(rand.Intn(50)+1),
		gocql.MinTimeUUID(time.Date(2021, 10, 18, 11, 12, 0, 0, time.Now().Location()))).Iter().Scanner()

	//checkUuid, _ := gocql.ParseUUID(`94f5afff-301a-11ec-9f64-1c1b0dff1b95`)
	//
	//scanner := session.Query(cql,
	//	int64(2),
	//	checkUuid).Iter().Scanner()

	for scanner.Next() {
		row := transaction.Transaction{}
		err := scanner.Scan(
			&row.TransactionUuid,
			&row.UserId,
			&row.CreateTime,
			&row.SourceWalletId,
			&row.TargetWalletId,
			&row.Amount,
			&row.PreviousBalance,
			&row.RateExchange,
			&row.SystemType,
			&row.SystemId)
		if err != nil {
			log.Fatal(err)
		}

		result = append(result, row)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	duration := time.Since(start)
	fmt.Println(duration)
	//for _, transaction := range result {
	//	fmt.Println(transaction)
	//}
}
