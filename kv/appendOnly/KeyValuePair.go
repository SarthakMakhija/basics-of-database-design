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

func DeserializeFrom(bytes []byte) KeyValuePair {
	return DeserializeFromOffset(bytes, 0)
}

func DeserializeFromOffset(bytes []byte, offset Offset) KeyValuePair {
	keyValueLogContent := (*keyValueLogContent)(unsafe.Pointer(&bytes[offset]))
	if keyValueLogContent.isEmpty() {
		return KeyValuePair{}
	}
	return KeyValuePair{
		Key:            keyValueLogContent.key,
		Value:          keyValueLogContent.value,
		startingOffset: offset,
	}
}

func DeserializeAll(bytes []byte) []KeyValuePair {
	var pairs []KeyValuePair

	var offset Offset = 0
	for offset < Offset(int64(len(bytes))) {
		keyValuePair := DeserializeFromOffset(bytes, offset)
		if keyValuePair.isEmpty() {
			break
		}
		pairs = append(pairs, keyValuePair)
		offset = offset + Offset(int64(KeyValueContentSize))
	}
	return pairs
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
