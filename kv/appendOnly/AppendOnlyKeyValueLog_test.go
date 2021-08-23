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
		Key:   []byte("Company"),
		Value: []byte("ThoughtWorks"),
	}

	log.Put(keyValuePair)

	readPair := log.GetAtStartingOffset(0)
	if !keyValuePair.ContentEquals(readPair) {
		t.Fatalf("Expected Key %v value %v, received key %v value %v", keyValuePair.Key, keyValuePair.Value, readPair.Key, readPair.Value)
	}
}

func TestPutGetSecondKeyPairToKeyValueLog(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	log := appendOnly.NewKeyValueLog(fileName)

	keyValuePair := appendOnly.KeyValuePair{
		Key:   []byte("Company"),
		Value: []byte("ThoughtWorks"),
	}
	anotherKeyValuePair := appendOnly.KeyValuePair{
		Key:   []byte("SectorSize"),
		Value: []byte("512B"),
	}

	log.Put(keyValuePair)
	log.Put(anotherKeyValuePair)

	readPair := log.GetAtStartingOffset(appendOnly.Offset(appendOnly.KeyValueContentSize))

	if !anotherKeyValuePair.ContentEquals(readPair) {
		t.Fatalf("Expected Key %v value %v, received key %v value %v", anotherKeyValuePair.Key, anotherKeyValuePair.Value, readPair.Key, readPair.Value)
	}
}

func TestReturnsTrueGivenItIsANewlyCreatedKeyValueLog(t *testing.T) {
	t.SkipNow()

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
		Key:   []byte("Company"),
		Value: []byte("ThoughtWorks"),
	}
	log.Put(keyValuePair)

	if log.IsANewlyCreatedKeyValueLog() != false {
		t.Fatalf("Expected false, received true")
	}
}
