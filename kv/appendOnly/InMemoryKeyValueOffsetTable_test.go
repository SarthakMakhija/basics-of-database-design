package appendOnly_test

import (
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
	inMemoryKeyValueTable.Put("sectorSize", "512B")

	value := inMemoryKeyValueTable.Get("sectorSize")
	expectedValue := "512B"

	if value != expectedValue {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}

func TestGetValueByNonExistentKey(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	emptyInMemoryKeyValueTable := appendOnly.NewInMemoryKeyValueOffsetTable(newKeyValueLog(fileName))

	value := emptyInMemoryKeyValueTable.Get("sectorSize")
	expectedValue := ""

	if value != expectedValue {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}
