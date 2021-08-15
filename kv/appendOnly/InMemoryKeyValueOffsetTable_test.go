package appendOnly_test

import (
	"bytes"
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"testing"
)

func newKeyValueLog(fileName string) *appendOnly.KeyValueLog {
	log := appendOnly.NewKeyValueLog(fileName)
	return &log
}

func TestPutValueByKey(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	inMemoryKeyValueTable := appendOnly.NewInMemoryKeyValueOffsetTable(newKeyValueLog(fileName))
	inMemoryKeyValueTable.Put([]byte("sectorSize"), []byte("512B"))

	value := inMemoryKeyValueTable.Get([]byte("sectorSize"))
	expectedValue := []byte("512B")

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}

func TestGetValueByNonExistentKey(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	emptyInMemoryKeyValueTable := appendOnly.NewInMemoryKeyValueOffsetTable(newKeyValueLog(fileName))

	value := emptyInMemoryKeyValueTable.Get([]byte("sectorSize"))
	var expectedValue []byte

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}

func TestReloadInMemoryKeyValueOffsetTableFromKeyValueLog(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	log := newKeyValueLog(fileName)
	log.Put(appendOnly.KeyValuePair{Key: []byte("Company"), Value: []byte("ThoughtWorks")})

	inMemoryKeyValueTable := appendOnly.ReloadFrom(log)
	value := inMemoryKeyValueTable.Get([]byte("Company"))

	expectedValue := []byte("ThoughtWorks")

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}

func TestReloadInMemoryKeyValueOffsetTableFromKeyValueLogWithMultipleKeyValues(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	log := newKeyValueLog(fileName)
	log.Put(appendOnly.KeyValuePair{Key: []byte("Company"), Value: []byte("ThoughtWorks")})
	log.Put(appendOnly.KeyValuePair{Key: []byte("Sector"), Value: []byte("454")})

	inMemoryKeyValueTable := appendOnly.ReloadFrom(log)
	value := inMemoryKeyValueTable.Get([]byte("Sector"))

	expectedValue := []byte("454")

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}
