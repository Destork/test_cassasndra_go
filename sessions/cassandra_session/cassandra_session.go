package cassandra_session

import (
	"github.com/gocql/gocql"
	"log"
	"sync"
	"time"
)

var once sync.Once
var instanceCassandra **gocql.Session
var clusterCassandraInstance **gocql.ClusterConfig

func GetCassandraSession() *gocql.Session {
	once.Do(func() {
		cluster := getCassandraCluster()
		session, err := cluster.CreateSession()
		if err != nil {
			log.Fatal(err)
		}

		instanceCassandra = &session
	})

	return *instanceCassandra
}

func getCassandraCluster() *gocql.ClusterConfig {
	if clusterCassandraInstance == nil {
		//hosts := []string{
		//	"95.216.25.222:9042",
		//}
		hosts := []string{
			"localhost:9042",
		}

		cluster := gocql.NewCluster(hosts...)
		cluster.Keyspace = "test"
		cluster.Consistency = gocql.Quorum
		cluster.Timeout = time.Second * 15

		clusterCassandraInstance = &cluster
	}

	return *clusterCassandraInstance
}

func ReconnectCassandraSession() *gocql.Session {
	CloseCassandraConnect()
	cluster := getCassandraCluster()
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	instanceCassandra = &session

	return *instanceCassandra
}

func CloseCassandraConnect() {
	(*instanceCassandra).Close()
}
