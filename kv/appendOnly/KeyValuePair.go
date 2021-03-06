package appendOnly

import (
	"bytes"
	"unsafe"
)

type KeyValuePair struct {
	Key            []byte
	Value          []byte
	startingOffset Offset
}

type keyValueLogContent struct {
	keySize   int
	key       []byte
	valueSize int
	value     []byte
}

func (keyValueLogContent keyValueLogContent) isEmpty() bool {
	return keyValueLogContent.keySize == 0
}

const KeyValueContentSize = unsafe.Sizeof(keyValueLogContent{})

func (keyValuePair KeyValuePair) Serialize() []byte {
	serialized := make([]byte, KeyValueContentSize)
	content := (*keyValueLogContent)(unsafe.Pointer(&serialized[0]))

	content.keySize = keyValuePair.keySize()
	content.key = keyValuePair.Key
	content.valueSize = keyValuePair.valueSize()
	content.value = keyValuePair.Value

	return serialized
}

func (keyValuePair KeyValuePair) keySize() int {
	return len(keyValuePair.Key)
}

func (keyValuePair KeyValuePair) valueSize() int {
	return len(keyValuePair.Value)
}

func (keyValuePair KeyValuePair) isEmpty() bool {
	return keyValuePair.keySize() == 0
}

func (keyValuePair KeyValuePair) HumanReadableKey() string {
	return string(keyValuePair.Key)
}

func (keyValuePair KeyValuePair) HumanReadableValue() string {
	return string(keyValuePair.Value)
}

func (keyValuePair KeyValuePair) ContentEquals(other KeyValuePair) bool {
	return bytes.Equal(keyValuePair.Key, other.Key) && bytes.Equal(keyValuePair.Value, other.Value)
}

type KeyValuePairIterator struct {
	bytes         []byte
	currentOffset Offset
}

type iterationFunction func(keyValuePair KeyValuePair)

func NewKeyValuePairIterator(bytes []byte) *KeyValuePairIterator {
	return NewKeyValuePairIteratorAt(0, bytes)
}

func NewKeyValuePairIteratorAt(offset Offset, bytes []byte) *KeyValuePairIterator {
	return &KeyValuePairIterator{
		bytes:         bytes,
		currentOffset: offset,
	}
}

func (keyValuePairIterator *KeyValuePairIterator) HasNext() bool {
	if keyValuePairIterator.offsetLessThanContentSize() {
		if keyValuePairIterator.keyValueLogContent().isEmpty() {
			return false
		}
		return true
	}
	return false
}

func (keyValuePairIterator *KeyValuePairIterator) Next() KeyValuePair {
	keyValueLogContent := keyValuePairIterator.keyValueLogContent()
	pair := KeyValuePair{
		Key:            keyValueLogContent.key,
		Value:          keyValueLogContent.value,
		startingOffset: keyValuePairIterator.currentOffset,
	}
	keyValuePairIterator.currentOffset = keyValuePairIterator.currentOffset + Offset(uint64(KeyValueContentSize))
	return pair
}

func (keyValuePairIterator *KeyValuePairIterator) All() []KeyValuePair {
	var pairs []KeyValuePair
	keyValuePairIterator.iterateWith(func(keyValuePair KeyValuePair) {
		pairs = append(pairs, keyValuePair)
	})
	return pairs
}

func (keyValuePairIterator *KeyValuePairIterator) NextStartingOffset() Offset {
	keyValuePairIterator.iterateWith(func(keyValuePair KeyValuePair) {})
	return keyValuePairIterator.currentOffset
}

func (keyValuePairIterator *KeyValuePairIterator) keyValueLogContent() *keyValueLogContent {
	return (*keyValueLogContent)(unsafe.Pointer(&keyValuePairIterator.bytes[keyValuePairIterator.currentOffset]))
}

func (keyValuePairIterator *KeyValuePairIterator) offsetLessThanContentSize() bool {
	return int(keyValuePairIterator.currentOffset) < len(keyValuePairIterator.bytes)
}

func (keyValuePairIterator *KeyValuePairIterator) iterateWith(fn iterationFunction) {
	for keyValuePairIterator.offsetLessThanContentSize() && keyValuePairIterator.HasNext() {
		fn(keyValuePairIterator.Next())
	}
}
