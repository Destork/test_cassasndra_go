package transaction

import (
	"database/sql"
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
	"test_cassandra_go/tools"
	"time"
)

type Transaction struct {
	TransactionUuid gocql.UUID
	UserId          int64
	CreateTime      time.Time
	SourceWalletId  int64
	TargetWalletId  int64
	Amount          *inf.Dec
	PreviousBalance *inf.Dec
	RateExchange    *inf.Dec
	SystemType      string
	SystemId        int64
}

func UnmarshalMysqlTransaction(scanner *sql.Rows) (Transaction, error) {
	row := Transaction{}

	var transactionUuidString, amountString, previousBalanceString, rateExchangeString string

	err := scanner.Scan(
		&transactionUuidString,
		&row.UserId,
		&row.CreateTime,
		&row.SourceWalletId,
		&row.TargetWalletId,
		&amountString,
		&previousBalanceString,
		&rateExchangeString,
		&row.SystemType,
		&row.SystemId)

	row.TransactionUuid, _ = gocql.ParseUUID(transactionUuidString)
	row.Amount, _ = tools.FromStringToDec(amountString)
	row.PreviousBalance, _ = tools.FromStringToDec(previousBalanceString)
	row.RateExchange, _ = tools.FromStringToDec(rateExchangeString)

	if err != nil {
		return Transaction{}, err
	}

	return row, nil
}
