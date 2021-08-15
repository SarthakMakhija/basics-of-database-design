package appendOnly_test

import (
	"bytes"
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"testing"
)

func TestPutValueByKeyInStore(t *testing.T) {
	fileName := "./keyValue.kvlog"
	store := appendOnly.Open(fileName)

	defer deleteFile(fileName)

	store.Put([]byte("Company"), []byte("Thoughtworks"))
	value := store.Get([]byte("Company"))
	expected := []byte("Thoughtworks")

	if !bytes.Equal(value, expected) {
		t.Fatalf("Expected %v, received %v", expected, value)
	}
}

func TestGetValueByNonExistentKeyInStore(t *testing.T) {
	fileName := "./keyValue.kvlog"
	store := appendOnly.Open(fileName)

	defer deleteFile(fileName)

	value := store.Get([]byte("NonExistentKey"))
	var expected []byte

	if !bytes.Equal(value, expected) {
		t.Fatalf("Expected %v, received %v", expected, value)
	}
}

func TestPutMultipleKeyValuesInStore(t *testing.T) {
	fileName := "./keyValue.kvlog"
	store := appendOnly.Open(fileName)

	defer deleteFile(fileName)

	store.Put([]byte("Company"), []byte("Thoughtworks"))
	store.Put([]byte("Region"), []byte("us-east-1"))

	company := store.Get([]byte("Company"))
	if !bytes.Equal(company, []byte("Thoughtworks")) {
		t.Fatalf("Expected %v, received %v", "Thoughtworks", company)
	}

	region := store.Get([]byte("Region"))
	if !bytes.Equal(region, []byte("us-east-1")) {
		t.Fatalf("Expected %v, received %v", "us-east-1", region)
	}
}
