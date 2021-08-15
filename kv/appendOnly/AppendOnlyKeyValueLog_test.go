package appendOnly_test

import (
	"bytes"
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

	readPair := log.GetFirst()
	if !bytes.Equal(readPair.Key, keyValuePair.Key) {
		t.Fatalf("Expected Key %v, received %v", keyValuePair.Key, readPair.Key)
	}

	if !bytes.Equal(readPair.Value, keyValuePair.Value) {
		t.Fatalf("Expected Value %v, received %v", keyValuePair.Value, readPair.Value)
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
	if !bytes.Equal(readPair.Key, anotherKeyValuePair.Key) {
		t.Fatalf("Expected Key %v, received %v", anotherKeyValuePair.Key, readPair.Key)
	}

	if !bytes.Equal(readPair.Value, anotherKeyValuePair.Value) {
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
		Key:   []byte("Company"),
		Value: []byte("ThoughtWorks"),
	}
	log.Put(keyValuePair)

	if log.IsANewlyCreatedKeyValueLog() != false {
		t.Fatalf("Expected false, received true")
	}
}
