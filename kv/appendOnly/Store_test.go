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

func TestPutMultipleKeyValuesInStoreGivenInMemoryTableGetsReloaded(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	log := newKeyValueLog(fileName)
	log.Put(appendOnly.KeyValuePair{Key: []byte("Company"), Value: []byte("ThoughtWorks")})
	log.Put(appendOnly.KeyValuePair{Key: []byte("Sector"), Value: []byte("454")})
	log.Put(appendOnly.KeyValuePair{Key: []byte("StoreType"), Value: []byte("KeyValue")})

	store := appendOnly.Open(fileName)
	value := store.Get([]byte("StoreType"))

	expectedValue := []byte("KeyValue")

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}

func TestGetKeyInStoreGivenInMemoryTableGetsReloadedAfterClose(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	store := appendOnly.Open(fileName)

	store.Put([]byte("Company"), []byte("ThoughtWorks"))
	store.Put([]byte("Sector"), []byte("454"))
	store.Put([]byte("StoreType"), []byte("KeyValue"))
	store.Close()

	reOpenedStore := appendOnly.Open(fileName)
	value := reOpenedStore.Get([]byte("StoreType"))

	expectedValue := []byte("KeyValue")

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}

func TestPutAKeyValueInStoreGivenInMemoryTableGetsReloadedAfterClose(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	store := appendOnly.Open(fileName)

	store.Put([]byte("Company"), []byte("ThoughtWorks"))
	store.Close()

	reOpenedStore := appendOnly.Open(fileName)
	reOpenedStore.Put([]byte("BranchLocation"), []byte("India"))

	value := reOpenedStore.Get([]byte("BranchLocation"))
	expectedValue := []byte("India")

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}

	value = reOpenedStore.Get([]byte("Company"))
	expectedValue = []byte("ThoughtWorks")

	if !bytes.Equal(value, expectedValue) {
		t.Fatalf("Expected %v, received %v", expectedValue, value)
	}
}
