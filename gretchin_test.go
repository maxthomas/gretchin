package gretchin

import (
	"testing"
	"time"

	"github.com/hltcoe/goncrete"
	"github.com/stretchr/testify/assert"
)

var (
	testMetadata = getMetadata()
)

func getMetadata() *goncrete.AnnotationMetadata {
	am := goncrete.NewAnnotationMetadata()
	am.Tool = "test"
	am.Timestamp = time.Now().Unix()
	return am
}

func TestCommsToBytes(t *testing.T) {
	var err error

	comm := goncrete.NewCommunication()
	comm.ID = "foo"
	uuid := goncrete.NewUUID()
	uuid.UuidString = "asdf"
	comm.UUID = uuid
	comm.Type = "none"
	comm.Metadata = testMetadata

	var bytez []byte
	bytez, err = toBytes(comm)
	assert.NoError(t, err)
	assert.True(t, len(bytez) > 0)

	// test multiple times
	for i := 0; i < 10; i++ {
		bytez, err = toBytes(comm)
		assert.NoError(t, err)
		assert.True(t, len(bytez) > 0)
	}

	// test reverse
	deserd, err := fromBytes(bytez)
	assert.NoError(t, err)
	assert.Equal(t, "none", deserd.Type)
	for i := 0; i < 10; i++ {
		deserd, err = fromBytes(bytez)
		assert.NoError(t, err)
		assert.Equal(t, "none", deserd.Type)
	}
}
