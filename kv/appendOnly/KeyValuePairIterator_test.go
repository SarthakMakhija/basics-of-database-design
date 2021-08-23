package appendOnly_test

import (
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"testing"
)

func TestReturnsTrueGivenNextKeyValuePairExists(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key: []byte("Sector"),
	}
	serialized := pair.Serialize()
	iterator := appendOnly.NewKeyValuePairIterator(serialized)
	hasNext := iterator.HasNext()

	if hasNext != true {
		t.Fatalf("Expected hasNext to be true, received false")
	}
}

func TestReturnsFalseGivenNoNextKeyValuePairExistsInEmptyBytes(t *testing.T) {
	iterator := appendOnly.NewKeyValuePairIterator([]byte{})
	hasNext := iterator.HasNext()

	if hasNext != false {
		t.Fatalf("Expected hasNext to be false, received true")
	}
}

func TestReturnsFalseGivenNoNextKeyValuePairExists(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key: []byte("Sector"),
	}
	serialized := pair.Serialize()
	iterator := appendOnly.NewKeyValuePairIterator(serialized)
	iterator.HasNext()
	iterator.Next()

	hasNext := iterator.HasNext()

	if hasNext != false {
		t.Fatalf("Expected hasNext to be false, received true")
	}
}

func TestReturnsNextKeyValuePair(t *testing.T) {
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

func TestReturnsNextStartingOffset(t *testing.T) {
	pair := appendOnly.KeyValuePair{
		Key: []byte("Sector"),
	}
	serialized := pair.Serialize()
	iterator := appendOnly.NewKeyValuePairIterator(serialized)
	nextStartingOffset := iterator.NextStartingOffset()
	nextExpectedOffset := appendOnly.Offset(appendOnly.KeyValueContentSize)

	if nextStartingOffset != nextExpectedOffset {
		t.Fatalf("Expected nextstartingoffset to be %v, received %v", nextExpectedOffset, nextStartingOffset)
	}
}

func TestReturnsNextStartingOffsetForEmptyBytes(t *testing.T) {
	iterator := appendOnly.NewKeyValuePairIterator([]byte{})
	nextStartingOffset := iterator.NextStartingOffset()
	nextExpectedOffset := appendOnly.Offset(0)

	if nextStartingOffset != nextExpectedOffset {
		t.Fatalf("Expected nextstartingoffset to be %v, received %v", nextExpectedOffset, nextStartingOffset)
	}
}
