package appendOnly_test

import (
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"os"
	"testing"
)

func deleteFile(fileName string) {
	os.Remove(fileName)
}

func TestWriteReadKeyPairToKeyValueLog(t *testing.T) {
	fileName := "./keyValue.kvlog"
	defer deleteFile(fileName)

	log := appendOnly.NewKeyValueLog(fileName)

	keyValuePair := appendOnly.KeyValuePair{
		Key:   "Company",
		Value: "ThoughtWorks",
	}

	log.Write(keyValuePair)

	readPair := log.ReadFirst()
	if readPair.Key != keyValuePair.Key {
		t.Fatalf("Expected Key %v, received %v", keyValuePair.Key, readPair.Key)
	}

	if readPair.Value != keyValuePair.Value {
		t.Fatalf("Expected Value %v, received %v", keyValuePair.Value, readPair.Value)
	}
}

func TestWriteReadSecondKeyPairToKeyValueLog(t *testing.T) {
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

	log.Write(keyValuePair)
	log.Write(anotherKeyValuePair)

	readPair := log.ReadAtStartingOffset(int64(appendOnly.KeyValueContentSize))
	if readPair.Key != anotherKeyValuePair.Key {
		t.Fatalf("Expected Key %v, received %v", anotherKeyValuePair.Key, readPair.Key)
	}

	if readPair.Value != anotherKeyValuePair.Value {
		t.Fatalf("Expected Value %v, received %v", anotherKeyValuePair.Value, readPair.Value)
	}
}
