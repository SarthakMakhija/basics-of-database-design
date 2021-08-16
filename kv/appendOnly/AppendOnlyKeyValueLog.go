package appendOnly

import (
	"gitlab.com/stone.code/assert"
	"os"
)

type Offset int64

type KeyValueLog struct {
	fileName              string
	file                  *os.File
	mappedBytes           []byte
	currentStartingOffset Offset
}

func NewKeyValueLog(fileName string) KeyValueLog {

	nextStartingOffset := func(bytes []byte) Offset {
		var offset Offset = 0
		for offset < Offset(int64(len(bytes))) {
			keyValuePair := DeserializeFromOffset(bytes, offset)
			if keyValuePair.isEmpty() {
				break
			}
			offset = offset + Offset(int64(KeyValueContentSize))
		}
		return offset
	}

	fileIO := CreateOrOpenReadWrite(fileName)
	bytes, isNew := fileIO.Mmap(fileIO.File, 4096)
	if fileIO.Err == nil {
		log := KeyValueLog{
			fileName:    fileName,
			file:        fileIO.File,
			mappedBytes: bytes,
		}
		if !isNew {
			log.currentStartingOffset = nextStartingOffset(bytes)
		}
		return log
	}
	return KeyValueLog{}
}

func (keyValueLog *KeyValueLog) Put(keyValuePair KeyValuePair) Offset {
	originalStartingOffset := keyValueLog.currentStartingOffset
	newStartingOffset := originalStartingOffset + Offset(keyValueLog.put(keyValuePair))
	keyValueLog.currentStartingOffset = newStartingOffset

	return originalStartingOffset
}

func (keyValueLog KeyValueLog) GetAtStartingOffset(offset Offset) KeyValuePair {
	return DeserializeFromOffset(keyValueLog.mappedBytes, offset)
}

func (keyValueLog KeyValueLog) IsANewlyCreatedKeyValueLog() bool {
	fileIO := NewFileIO()
	if fileIO.FileSize(keyValueLog.fileName) > 0 {
		return false
	}
	return true
}

func (keyValueLog *KeyValueLog) Close() {
	fileIO := NewFileIO()
	fileIO.File = keyValueLog.file

	fileIO.Munmap(keyValueLog.mappedBytes)
	assert.Assert(fileIO.Err == nil, "FileIO must not contain any Error after unmap")
	fileIO.CloseSilently()
}

func CreateOrOpenReadWrite(fileName string) *MutableFileIO {
	fileIO := NewFileIO()
	fileIO.CreateOrOpenReadWrite(fileName)
	return fileIO
}

func (keyValueLog *KeyValueLog) put(keyValuePair KeyValuePair) int {
	bytes := keyValuePair.Serialize()
	return copy(keyValueLog.mappedBytes[keyValueLog.currentStartingOffset:], bytes)
}
