package appendOnly_test

import (
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"testing"
)

func TestSerializeKey(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key: []byte("Sector"),
	}
	serialized := pair.Serialize()
	iterator := appendOnly.NewKeyValuePairIterator(serialized)
	deserializedPair := iterator.Next()

	if !pair.ContentEquals(deserializedPair) {
		t.Fatalf("Expected Key %v, received %v", pair.Key, deserializedPair.Key)
	}
}

func TestSerializeValue(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Value: []byte("512B"),
	}
	serialized := pair.Serialize()
	iterator := appendOnly.NewKeyValuePairIterator(serialized)
	deserializedPair := iterator.Next()

	if !pair.ContentEquals(deserializedPair) {
		t.Fatalf("Expected Value %v, received %v", pair.Value, deserializedPair.Value)
	}
}

func TestSerializeKeyValue(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key:   []byte("Sector"),
		Value: []byte("512B"),
	}
	serialized := pair.Serialize()
	iterator := appendOnly.NewKeyValuePairIterator(serialized)
	deserializedPair := iterator.Next()

	if !pair.ContentEquals(deserializedPair) {
		t.Fatalf("Expected Key %v value %v, received key %v value %v", pair.Key, pair.Value, deserializedPair.Key, deserializedPair.Value)
	}
}

func TestSerializeDeserializeAll(t *testing.T) {
	firstPair := appendOnly.KeyValuePair{
		Key:   []byte("Sector"),
		Value: []byte("512B"),
	}
	firstByteArray := firstPair.Serialize()

	secondPair := appendOnly.KeyValuePair{
		Key:   []byte("Name"),
		Value: []byte("Sarthak"),
	}
	secondByteArray := secondPair.Serialize()

	thirdPair := appendOnly.KeyValuePair{
		Key:   []byte("Sector"),
		Value: []byte("345"),
	}
	thirdByteArray := thirdPair.Serialize()

	var finalByteArray []byte
	finalByteArray = append(finalByteArray, firstByteArray...)
	finalByteArray = append(finalByteArray, secondByteArray...)
	finalByteArray = append(finalByteArray, thirdByteArray...)

	iterator := appendOnly.NewKeyValuePairIterator(finalByteArray)
	allPairs := iterator.All()

	if !firstPair.ContentEquals(allPairs[0]) {
		t.Fatalf("Expected Key %v value %v, received key %v value %v", firstPair.Key, firstPair.Value, allPairs[0].Key, allPairs[0].Value)
	}
	if !secondPair.ContentEquals(allPairs[1]) {
		t.Fatalf("Expected Key %v value %v, received key %v value %v", secondPair.Key, secondPair.Value, allPairs[1].Key, allPairs[1].Value)
	}
	if !thirdPair.ContentEquals(allPairs[2]) {
		t.Fatalf("Expected Key %v value %v, received key %v value %v", thirdPair.Key, thirdPair.Value, allPairs[2].Key, allPairs[2].Value)
	}
}

func TestSerializeLargeKeyValue(t *testing.T) {
	longContent := func(prefix string) []byte {
		key := []byte(prefix)
		for count := 1; count <= 1024; count++ {
			key = append(key, '0')
		}
		return key
	}
	pair := appendOnly.KeyValuePair{
		Key:   longContent("Key"),
		Value: longContent("Value"),
	}
	serialized := pair.Serialize()
	iterator := appendOnly.NewKeyValuePairIterator(serialized)
	deserializedPair := iterator.Next()

	if !pair.ContentEquals(deserializedPair) {
		t.Fatalf("Expected Key %v value %v, received key %v value %v", pair.Key, pair.Value, deserializedPair.Key, deserializedPair.Value)
	}
}

func TestHumanReadableKey(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key: []byte("Sector"),
	}
	humanReadableKey := pair.HumanReadableKey()
	expected := "Sector"

	if expected != humanReadableKey {
		t.Fatalf("Expected human readable key to be %v, received %v", expected, humanReadableKey)
	}
}

func TestHumanReadableValue(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Value: []byte("512B"),
	}
	humanReadableValue := pair.HumanReadableValue()
	expected := "512B"

	if expected != humanReadableValue {
		t.Fatalf("Expected human readable value to be %v, received %v", expected, humanReadableValue)
	}
}
