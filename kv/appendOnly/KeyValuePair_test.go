package appendOnly_test

import (
	"bytes"
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"testing"
)

func TestSerializeKey(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key: []byte("Sector"),
	}
	serialized := pair.Serialize()
	deserializedPair := appendOnly.DeserializeFrom(serialized)

	if !bytes.Equal(deserializedPair.Key, pair.Key) {
		t.Fatalf("Expected Key %v, received %v", pair.Key, deserializedPair.Key)
	}
}

func TestSerializeValue(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Value: []byte("512B"),
	}
	serialized := pair.Serialize()
	deserializedPair := appendOnly.DeserializeFrom(serialized)

	if !bytes.Equal(deserializedPair.Value, pair.Value) {
		t.Fatalf("Expected Value %v, received %v", pair.Value, deserializedPair.Value)
	}
}

func TestSerializeKeyValue(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key:   []byte("Sector"),
		Value: []byte("512B"),
	}
	serialized := pair.Serialize()
	deserializedPair := appendOnly.DeserializeFrom(serialized)

	if !bytes.Equal(deserializedPair.Key, pair.Key) {
		t.Fatalf("Expected Key %v, received %v", pair.Key, deserializedPair.Key)
	}

	if !bytes.Equal(deserializedPair.Value, pair.Value) {
		t.Fatalf("Expected Value %v, received %v", pair.Value, deserializedPair.Value)
	}
}
