package appendOnly_test

import (
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"os"
	"testing"
)

func deleteFile(fileName string) {
	_ = os.Remove(fileName)
}

func TestPutGetKeyPairToKeyValueLog(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	log := appendOnly.NewKeyValueLog(fileName)

	keyValuePair := appendOnly.KeyValuePair{
		Key:   "Company",
		Value: "ThoughtWorks",
	}

	log.Put(keyValuePair)

	readPair := log.GetFirst()
	if readPair.Key != keyValuePair.Key {
		t.Fatalf("Expected Key %v, received %v", keyValuePair.Key, readPair.Key)
	}

	if readPair.Value != keyValuePair.Value {
		t.Fatalf("Expected Value %v, received %v", keyValuePair.Value, readPair.Value)
	}
}

func TestPutGetSecondKeyPairToKeyValueLog(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	log := appendOnly.NewKeyValueLog(fileName)

	keyValuePair := appendOnly.KeyValuePair{
		Key:   "Company",
		Value: "ThoughtWorks",
	}
	anotherKeyValuePair := appendOnly.KeyValuePair{
		Key:   "SectorSize",
		Value: "512B",
	}

	log.Put(keyValuePair)
	log.Put(anotherKeyValuePair)

	readPair := log.GetAtStartingOffset(appendOnly.Offset(appendOnly.KeyValueContentSize))
	if readPair.Key != anotherKeyValuePair.Key {
		t.Fatalf("Expected Key %v, received %v", anotherKeyValuePair.Key, readPair.Key)
	}

	if readPair.Value != anotherKeyValuePair.Value {
		t.Fatalf("Expected Value %v, received %v", anotherKeyValuePair.Value, readPair.Value)
	}
}

func TestReturnsTrueGivenItIsANewlyCreatedKeyValueLog(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	log := appendOnly.NewKeyValueLog(fileName)
	if log.IsANewlyCreatedKeyValueLog() != true {
		t.Fatalf("Expected true, received false")
	}
}

func TestReturnsFalseGivenItIsNotANewlyCreatedKeyValueLog(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	log := appendOnly.NewKeyValueLog(fileName)
	keyValuePair := appendOnly.KeyValuePair{
		Key:   "Company",
		Value: "ThoughtWorks",
	}
	log.Put(keyValuePair)

	if log.IsANewlyCreatedKeyValueLog() != false {
		t.Fatalf("Expected false, received true")
	}
}
