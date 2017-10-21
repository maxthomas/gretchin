package gretchin

import (
	"errors"
	"sync"

	"git.apache.org/thrift.git/lib/go/thrift"

	"github.com/go-redis/redis"
	"github.com/hltcoe/goncrete"
)

type Gretchin struct {
	redisClient redis.UniversalClient
}

var _ goncrete.FetchCommunicationService = (*Gretchin)(nil)
var _ goncrete.StoreCommunicationService = (*Gretchin)(nil)

var (
	deserPool = sync.Pool{New: func() interface{} {
		dser := thrift.NewTDeserializer()
		mcompact := thrift.NewTCompactProtocol(dser.Transport)
		dser.Protocol = mcompact
		return dser
	},
	}

	serPool = sync.Pool{New: func() interface{} {
		ser := thrift.NewTSerializer()
		mcompact := thrift.NewTCompactProtocol(ser.Transport)
		ser.Protocol = mcompact
		return ser
	},
	}
)

func NewGretchin(rcli redis.UniversalClient) *Gretchin {
	return &Gretchin{redisClient: rcli}
}

func toBytes(comm *goncrete.Communication) ([]byte, error) {
	ser := serPool.Get().(*thrift.TSerializer)
	defer serPool.Put(ser)
	ser.Transport.Reset()
	return ser.Write(comm)
}

func fromBytes(commBytes []byte) (*goncrete.Communication, error) {
	dser := deserPool.Get().(*thrift.TDeserializer)
	defer deserPool.Put(dser)

	comm := goncrete.NewCommunication()
	err := dser.Read(comm, commBytes)
	return comm, err
}

func (g *Gretchin) Alive() (bool, error) {
	return true, nil
}

func (g *Gretchin) About() (*goncrete.ServiceInfo, error) {
	si := goncrete.NewServiceInfo()
	si.Name = "gretchin"
	si.Version = "0.0.1"
	return si, nil
}

func (g *Gretchin) Fetch(req *goncrete.FetchRequest) (*goncrete.FetchResult_, error) {
	fr := goncrete.NewFetchResult_()
	fr.Communications = make([]*goncrete.Communication, 0)
	for _, cid := range req.GetCommunicationIds() {
		commBytes, err := g.redisClient.Get(cid).Bytes()
		if err != nil {
			return nil, err
		}

		comm, err := fromBytes(commBytes)
		if err != nil {
			return nil, err
		}

		fr.Communications = append(fr.Communications, comm)
	}
	return fr, nil
}

func (g *Gretchin) GetCommunicationCount() (int64, error) {
	return 0, errors.New("not supported")
}

func (g *Gretchin) GetCommunicationIDs(off int64, count int64) ([]string, error) {
	return nil, errors.New("not supported")
}

func (g *Gretchin) Store(comm *goncrete.Communication) error {
	commBytes, err := toBytes(comm)
	if err != nil {
		return err
	}

	return g.redisClient.Set(comm.ID, commBytes, 0).Err()
}

func DefaultTransportFactory() thrift.TTransportFactory {
	return thrift.NewTFramedTransportFactory(thrift.NewTBufferedTransportFactory(8192))
}

func DefaultProtocolFactory() thrift.TProtocolFactory {
	return thrift.NewTCompactProtocolFactory()
}
