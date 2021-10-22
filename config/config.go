package config

import (
	"context"
	"sync"
)

var ctx *context.Context
var engine string
var once sync.Once

func GetContext() context.Context {
	once.Do(func() {
		tmpCtx := context.Background()
		ctx = &tmpCtx
	})
	return *ctx
}

func GetEngine() string {
	once.Do(func() {
		engine = `cassandra`
		//engine = `mysql`
	})
	return engine
}
