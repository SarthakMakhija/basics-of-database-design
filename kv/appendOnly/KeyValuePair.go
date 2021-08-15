package appendOnly

import (
	"unsafe"
)

type KeyValuePair struct {
	Key   string
	Value string
}

type keyValueLogContent struct {
	keySize   int
	key       string
	valueSize int
	value     string
}

const size = unsafe.Sizeof(keyValueLogContent{})

func (keyValuePair KeyValuePair) Serialize() []byte {
	bytes := make([]byte, size)
	content := (*keyValueLogContent)(unsafe.Pointer(&bytes[0]))

	content.keySize = keyValuePair.keySize()
	content.key = keyValuePair.Key
	content.valueSize = keyValuePair.valueSize()
	content.value = keyValuePair.Value

	return bytes
}

func DeserializeFrom(bytes []byte) KeyValuePair {
	keyValueLogContent := (*keyValueLogContent)(unsafe.Pointer(&bytes[0]))
	return KeyValuePair{
		Key:   keyValueLogContent.key,
		Value: keyValueLogContent.value,
	}
}

func (keyValuePair KeyValuePair) keySize() int {
	return len(keyValuePair.Key)
}

func (keyValuePair KeyValuePair) valueSize() int {
	return len(keyValuePair.Value)
}
