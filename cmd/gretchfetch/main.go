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
		redisServer = pflag.String("redis-server", "localhost", "redis server to connect to")
		redisPort   = pflag.Int("redis-port", 6379, "redis port to connect to")
		fetchServer = pflag.String("fetch-server", "localhost", "fetch server to listen on")
		fetchPort   = pflag.Int("fetch-port", 9099, "fetch port to listen on")
	)

	pflag.Parse()
	if *redisServer == "" || *fetchServer == "" {
		pflag.Usage()
		return
	}

	redisCfg := &redis.Options{Addr: *redisServer + strconv.Itoa(*redisPort)}
	redisCli := redis.NewClient(redisCfg)
	defer redisCli.Close()
	logger.Info("connected to redis")

	gr := gretchin.NewGretchin(redisCli)
	srvString := *fetchServer + ":" + strconv.Itoa(*fetchPort)
	socket, err := thrift.NewTServerSocket(srvString)
	if err != nil {
		logger.Fatal("error during socket setup", zap.Error(err))
	}

	transFactory := gretchin.DefaultTransportFactory()
	protoFactory := gretchin.DefaultProtocolFactory()
	proc := goncrete.NewFetchCommunicationServiceProcessor(gr)
	srvr := thrift.NewTSimpleServer4(proc, socket, transFactory, protoFactory)
	logger.Info("server preparing to serve", zap.String("address", srvString))
	if err = srvr.Serve(); err != nil {
		logger.Error("Error during server", zap.Error(err))
	}
}
