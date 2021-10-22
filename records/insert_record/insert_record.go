package insert_record

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"test_cassandra_go/config"
	"test_cassandra_go/sessions/cassandra_session"
	"test_cassandra_go/sessions/mysql_session"
	"test_cassandra_go/transaction"
)

func InsertRecord(transaction transaction.Transaction) bool {
	var isOk bool
	if config.GetEngine() == `cassandra` {
		isOk = InsertCassandraTransaction(transaction)
	} else {
		isOk = InsertMysqlTransaction(transaction)
	}
	return isOk
}

func InsertRecords(transactions []transaction.Transaction) bool {
	var isOk bool
	if config.GetEngine() == `cassandra` {
		isOk = InsertCassandraRecords(transactions)
	} else {
		isOk = InsertMysqlRecords(transactions)
	}
	return isOk
}

func InsertCassandraTransaction(tr transaction.Transaction) bool {
	cql := `INSERT INTO transaction (
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
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	session := cassandra_session.GetCassandraSession()

	err := session.Query(
		cql,
		tr.TransactionUuid,
		tr.UserId,
		tr.CreateTime,
		tr.SourceWalletId,
		tr.TargetWalletId,
		tr.Amount,
		tr.PreviousBalance,
		tr.RateExchange,
		tr.SystemType,
		tr.SystemId).WithContext(config.GetContext()).Exec()

	if err != nil {
		fmt.Println(err)
		return false
	}

	fmt.Println(`insert record`)
	return true
}

func InsertMysqlTransaction(tr transaction.Transaction) bool {
	sql := `INSERT INTO transaction (
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
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	prepare, err := mysql_session.GetMysqlConnect().Prepare(sql)

	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	_, err = prepare.Exec(
		tr.TransactionUuid.String(),
		tr.UserId,
		tr.CreateTime,
		tr.SourceWalletId,
		tr.TargetWalletId,
		tr.Amount.String(),
		tr.PreviousBalance.String(),
		tr.RateExchange.String(),
		tr.SystemType,
		tr.SystemId)

	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	fmt.Println(`insert record`)
	return true
}

func InsertMysqlRecords(transactions []transaction.Transaction) bool {
	db := mysql_session.GetMysqlConnect()

	tx, err := db.BeginTx(config.GetContext(), nil)
	if err != nil {
		log.Fatal(err)
		return false
	}

	for _, tr := range transactions {
		_, err = tx.ExecContext(config.GetContext(),
			`
			INSERT INTO transaction (
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
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			tr.TransactionUuid.String(),
			tr.UserId,
			tr.CreateTime,
			tr.SourceWalletId,
			tr.TargetWalletId,
			tr.Amount.String(),
			tr.PreviousBalance.String(),
			tr.RateExchange.String(),
			tr.SystemType,
			tr.SystemId)
		if err != nil {
			log.Println(`ExecContext error`, err.Error())
			err := tx.Rollback()
			if err != nil {
				log.Println(`Rollback error`, err.Error())
				return false
			}
			return false
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Println(`Commit error`, err.Error())
		return false
	}
	log.Println(`commit transactions`)
	return true
}

func InsertCassandraRecords(transactions []transaction.Transaction) bool {
	session := cassandra_session.GetCassandraSession()

	b := session.NewBatch(gocql.UnloggedBatch).WithContext(config.GetContext())

	for _, tr := range transactions {
		b.Entries = append(b.Entries, gocql.BatchEntry{
			Stmt: `
				INSERT INTO transaction (
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
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			Args: []interface{}{
				tr.TransactionUuid,
				tr.UserId,
				tr.CreateTime,
				tr.SourceWalletId,
				tr.TargetWalletId,
				tr.Amount,
				tr.PreviousBalance,
				tr.RateExchange,
				tr.SystemType,
				tr.SystemId},
			Idempotent: true,
		})
	}

	err := session.ExecuteBatch(b)
	if err != nil {
		log.Println(`ExecuteBatch error`, err)
		return false
	}

	log.Println(`commit transactions`)
	return true
}