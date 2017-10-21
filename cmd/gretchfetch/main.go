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
		fetchServerAddress = pflag.String("fetch-server", "localhost:9099", "fetch address host:port")
	)

	pflag.Parse()
	if *redisServer == "" {
		pflag.Usage()
		return
	}

	redisCfg := &redis.Options{Addr: *redisServer + strconv.Itoa(*redisPort)}
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
	socket, err := thrift.NewTServerSocket(*fetchServerAddress)
	if err != nil {
		logger.Fatal("error during socket setup", zap.Error(err))
	}

	transFactory := gretchin.DefaultTransportFactory()
	protoFactory := gretchin.DefaultProtocolFactory()
	proc := goncrete.NewFetchCommunicationServiceProcessor(gr)
	srvr := thrift.NewTSimpleServer4(proc, socket, transFactory, protoFactory)
	logger.Info("server preparing to serve", zap.String("address", *fetchServerAddress))
	if err = srvr.Serve(); err != nil {
		logger.Error("Error during server", zap.Error(err))
	}
}
