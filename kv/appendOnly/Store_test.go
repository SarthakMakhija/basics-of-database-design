package appendOnly_test

import (
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"testing"
)

func TestPutValueByKeyInStore(t *testing.T) {
	fileName := "./keyValue.kvlog"
	store := appendOnly.Open(fileName)

	defer deleteFile(fileName)

	store.Put("Company", "Thoughtworks")
	value := store.Get("Company")
	expected := "Thoughtworks"

	if value != expected {
		t.Fatalf("Expected %v, received %v", expected, value)
	}
}

func TestGetValueByNonExistentKeyInStore(t *testing.T) {
	fileName := "./keyValue.kvlog"
	store := appendOnly.Open(fileName)

	defer deleteFile(fileName)

	value := store.Get("NonExistentKey")
	expected := ""

	if value != expected {
		t.Fatalf("Expected %v, received %v", expected, value)
	}
}

func TestPutMultipleKeyValuesInStore(t *testing.T) {
	fileName := "./keyValue.kvlog"
	store := appendOnly.Open(fileName)

	defer deleteFile(fileName)

	store.Put("Company", "Thoughtworks")
	store.Put("Region", "us-east-1")

	company := store.Get("Company")
	if company != "Thoughtworks" {
		t.Fatalf("Expected %v, received %v", "Thoughtworks", company)
	}

	region := store.Get("Region")
	if region != "us-east-1" {
		t.Fatalf("Expected %v, received %v", "us-east-1", region)
	}
}
