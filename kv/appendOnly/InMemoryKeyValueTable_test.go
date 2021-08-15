package appendOnly_test

import (
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"testing"
)

func TestPutValueByKey(t *testing.T) {
	inMemoryKeyValueTable := appendOnly.NewInMemoryKeyValueTable()
	inMemoryKeyValueTable.Put("sectorSize", "512B")

	value := inMemoryKeyValueTable.Get("sectorSize")
	expectedValue := "512B"

	if value != expectedValue {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}

func TestGetValueByNonExistentKey(t *testing.T) {
	emptyInMemoryKeyValueTable := appendOnly.NewInMemoryKeyValueTable()

	value := emptyInMemoryKeyValueTable.Get("sectorSize")
	expectedValue := ""

	if value != expectedValue {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}
