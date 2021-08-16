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
	iterator := appendOnly.NewKeyValuePairIterator(serialized)
	deserializedPair := iterator.Next()

	if !bytes.Equal(deserializedPair.Key, pair.Key) {
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
	iterator := appendOnly.NewKeyValuePairIterator(serialized)
	deserializedPair := iterator.Next()

	if !bytes.Equal(deserializedPair.Key, pair.Key) {
		t.Fatalf("Expected Key %v, received %v", pair.Key, deserializedPair.Key)
	}

	if !bytes.Equal(deserializedPair.Value, pair.Value) {
		t.Fatalf("Expected Value %v, received %v", pair.Value, deserializedPair.Value)
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

	if !bytes.Equal(allPairs[0].Key, firstPair.Key) {
		t.Fatalf("Expected Key %v, received %v", firstPair.Key, allPairs[0].Key)
	}
	if !bytes.Equal(allPairs[0].Value, firstPair.Value) {
		t.Fatalf("Expected Key %v, received %v", firstPair.Value, allPairs[0].Value)
	}

	if !bytes.Equal(allPairs[1].Key, secondPair.Key) {
		t.Fatalf("Expected Key %v, received %v", secondPair.Key, allPairs[1].Key)
	}
	if !bytes.Equal(allPairs[1].Value, secondPair.Value) {
		t.Fatalf("Expected Key %v, received %v", secondPair.Value, allPairs[1].Value)
	}

	if !bytes.Equal(allPairs[2].Key, thirdPair.Key) {
		t.Fatalf("Expected Key %v, received %v", thirdPair.Key, allPairs[2].Key)
	}
	if !bytes.Equal(allPairs[2].Value, thirdPair.Value) {
		t.Fatalf("Expected Key %v, received %v", thirdPair.Value, allPairs[2].Value)
	}
}
