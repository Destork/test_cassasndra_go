package main

import (
	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
	"sync"
	"test_cassandra_go/kafka_messages"
	"test_cassandra_go/records/insert_record"
	"test_cassandra_go/tools"
	"test_cassandra_go/transaction"
	"time"
)

func main() {
	//getMysqlRecords()
	//return

	//getCassandraRecords()
	//return

	messageThreads := 2

	messageCh := make(chan string, messageThreads)

	transactionThreads := 10

	transactionCh := make(chan []transaction.Transaction, transactionThreads)

	var wgTransaction, wgMessage sync.WaitGroup

	go func() {
		// read messages for Kafka
		kafka_messages.ReadMessageFromKafka(messageCh)
		close(messageCh)
	}()

	for tMessage := 0; tMessage < messageThreads; tMessage++ {
		wgMessage.Add(1)
		go func() {
			for _ = range messageCh {
				// process message into Transaction
			}
			close(transactionCh)
			wgMessage.Done()
		}()
	}

	for tTransaction := 0; tTransaction < transactionThreads; tTransaction++ {
		wgTransaction.Add(1)
		go func() {
			for transactions := range transactionCh {
				var doInsertRecords func(transactions []transaction.Transaction) bool

				doInsertRecords = func(transactions []transaction.Transaction) bool {
					isOk := insert_record.InsertRecords(transactions)
					if !isOk {
						return doInsertRecords(transactions)
					}

					return true
				}

				doInsertRecords(transactions)
				//send success to Kafka
			}
			wgTransaction.Done()
		}()
	}

	wgMessage.Wait()
	wgTransaction.Wait()

	//rand.Seed(time.Now().UnixNano())
	//defer func() {
	//	if Engine == `cassandra` {
	//		CloseCassandraConnect()
	//	} else {
	//		CloseMysqlConnect()
	//	}
	//}()
	//
	//success := make(chan bool)
	//errorChanel := make(chan Transaction)
	//
	//runtime.GOMAXPROCS(500)
	//
	//activeRoutines := 0
	//
	//lastConnectTime := time.Now()
	//
	//var n int
	//if engine == `cassandra` {
	//	n = 200000
	//} else {
	//	n = 2
	//}
	//
	//for i := 0; i < n; i++ {
	//	activeRoutines++
	//	go fillTestData(success, errorChanel)
	//}
	//for {
	//	select {
	//	case <-success:
	//		activeRoutines--
	//		if activeRoutines == 0 {
	//			return
	//		}
	//	case errorTransaction := <-errorChanel:
	//		fmt.Println(`errorChanel transaction`, errorTransaction)
	//		if engine == `cassandra` {
	//			newLastConnectTime := time.Now()
	//			if newLastConnectTime.Sub(lastConnectTime).Seconds() > 5 {
	//				lastConnectTime = newLastConnectTime
	//				ReconnectCassandraSession()
	//			}
	//		}
	//		go func() {
	//			var isOk bool
	//			if engine == `cassandra` {
	//				isOk = InsertCassandraTransaction(errorTransaction)
	//			} else {
	//				isOk = InsertMysqlTransaction(errorTransaction)
	//			}
	//			if isOk {
	//				success <- true
	//			} else {
	//				errorChanel <- errorTransaction
	//			}
	//		}()
	//	}
	//}
}

func fillTestData(success chan bool, error chan transaction.Transaction) {
	tr := getTestTransaction()
	isOk := insert_record.InsertRecord(tr)

	if isOk {
		success <- true
	} else {
		error <- tr
	}
}

func getTestTransaction() transaction.Transaction {
	return transaction.Transaction{
		TransactionUuid: gocql.TimeUUID(),
		UserId:          tools.RandInt64(1, 50),
		CreateTime:      time.Now(),
		SourceWalletId:  tools.RandInt64(1, 20),
		TargetWalletId:  tools.RandInt64(1, 20),
		Amount:          tools.RandDecimal(inf.NewDec(1, 9), inf.NewDec(999999999999999999, 9)),
		PreviousBalance: tools.RandDecimal(inf.NewDec(1, 9), inf.NewDec(999999999999999999, 9)),
		RateExchange:    tools.RandDecimal(inf.NewDec(1, 9), inf.NewDec(999999999999999999, 9)),
		SystemType:      `exchange_order`,
		SystemId:        int64(1),
	}
}
