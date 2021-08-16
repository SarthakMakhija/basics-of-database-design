package appendOnly

import (
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
	bytes := make([]byte, KeyValueContentSize)
	content := (*keyValueLogContent)(unsafe.Pointer(&bytes[0]))

	content.keySize = keyValuePair.keySize()
	content.key = keyValuePair.Key
	content.valueSize = keyValuePair.valueSize()
	content.value = keyValuePair.Value

	return bytes
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

type KeyValuePairIterator struct {
	bytes         []byte
	currentOffset Offset
}

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
	if keyValuePairIterator.keyValueLogContent().isEmpty() {
		return false
	}
	return true
}

func (keyValuePairIterator *KeyValuePairIterator) Next() KeyValuePair {
	keyValueLogContent := keyValuePairIterator.keyValueLogContent()
	pair := KeyValuePair{
		Key:            keyValueLogContent.key,
		Value:          keyValueLogContent.value,
		startingOffset: keyValuePairIterator.currentOffset,
	}
	keyValuePairIterator.currentOffset = keyValuePairIterator.currentOffset + Offset(int64(KeyValueContentSize))
	return pair
}

func (keyValuePairIterator *KeyValuePairIterator) All() []KeyValuePair {
	var pairs []KeyValuePair
	for int(keyValuePairIterator.currentOffset) < len(keyValuePairIterator.bytes) && keyValuePairIterator.HasNext() {
		pairs = append(pairs, keyValuePairIterator.Next())
	}
	return pairs
}

func (keyValuePairIterator *KeyValuePairIterator) NextStartingOffset() Offset {
	for int(keyValuePairIterator.currentOffset) < len(keyValuePairIterator.bytes) && keyValuePairIterator.HasNext() {
		keyValuePairIterator.Next()
	}
	return keyValuePairIterator.currentOffset
}

func (keyValuePairIterator *KeyValuePairIterator) keyValueLogContent() *keyValueLogContent {
	return (*keyValueLogContent)(unsafe.Pointer(&keyValuePairIterator.bytes[keyValuePairIterator.currentOffset]))
}
