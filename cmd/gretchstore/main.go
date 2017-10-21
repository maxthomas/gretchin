package main

import (
	"strconv"

	"git.apache.org/thrift.git/lib/go/thrift"

	"github.com/go-redis/redis"
	"github.com/hltcoe/goncrete"
	"github.com/maxthomas/gretchin"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	logger, _ = zap.NewProduction()
)

func main() {
	var (
		redisServer        = pflag.String("redis-server", "localhost", "redis server to connect to")
		redisPort          = pflag.Int("redis-port", 6379, "redis port to connect to")
		redisPassword      = pflag.String("redis-password", "", "redis password")
		storeServerAddress = pflag.String("store-server", "localhost:12999", "store address host:port")
	)

	pflag.Parse()
	if *storeServerAddress == "" {
		pflag.Usage()
		return
	}

	redisCfg := &redis.Options{Addr: *redisServer + ":" + strconv.Itoa(*redisPort)}
	if *redisPassword != "" {
		redisCfg.Password = *redisPassword
	}
	redisCli := redis.NewClient(redisCfg)
	defer redisCli.Close()
	if err := redisCli.Ping().Err(); err != nil {
		logger.Fatal("failed redis connect", zap.Error(err))
	}
	logger.Info("connected to redis")

	gr := gretchin.NewGretchin(redisCli)

	protoF := gretchin.DefaultProtocolFactory()
	transF := gretchin.DefaultTransportFactory()

	socket, err := thrift.NewTServerSocket(*storeServerAddress)
	if err != nil {
		logger.Fatal("error during socket setup", zap.Error(err))
	}
	proc := goncrete.NewStoreCommunicationServiceProcessor(gr)
	srvr := thrift.NewTSimpleServer4(proc, socket, transF, protoF)
	logger.Info("server preparing to serve", zap.String("address", *storeServerAddress))
	if err = srvr.Serve(); err != nil {
		logger.Error("Error during server", zap.Error(err))
	}
}
