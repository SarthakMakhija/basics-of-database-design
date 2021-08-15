package appendOnly_test

import (
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"testing"
)

func TestSerializeKey(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key: "Sector",
	}
	bytes := pair.Serialize()
	deserializedPair := appendOnly.DeserializeFrom(bytes)

	if deserializedPair.Key != pair.Key {
		t.Fatalf("Expected Key %v, received %v", pair.Key, deserializedPair.Key)
	}
}

func TestSerializeValue(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Value: "512B",
	}
	bytes := pair.Serialize()
	deserializedPair := appendOnly.DeserializeFrom(bytes)

	if deserializedPair.Value != pair.Value {
		t.Fatalf("Expected Value %v, received %v", pair.Value, deserializedPair.Value)
	}
}

func TestSerializeKeyValue(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key:   "Sector",
		Value: "512B",
	}
	bytes := pair.Serialize()
	deserializedPair := appendOnly.DeserializeFrom(bytes)

	if deserializedPair.Key != pair.Key {
		t.Fatalf("Expected Key %v, received %v", pair.Key, deserializedPair.Key)
	}

	if deserializedPair.Value != pair.Value {
		t.Fatalf("Expected Value %v, received %v", pair.Value, deserializedPair.Value)
	}
}
