package appendOnly

import (
	"gitlab.com/stone.code/assert"
	"os"
	"syscall"
)

type Offset int64

type KeyValueLog struct {
	fileName              string
	file                  *os.File
	mappedBytes           []byte
	currentStartingOffset Offset
}

func NewKeyValueLog(fileName string) KeyValueLog {
	fileIO := createOrOpen(fileName)
	bytes := fileIO.Mmap(fileIO.File, 4096)
	if fileIO.Err == nil {
		return KeyValueLog{
			fileName:    fileName,
			file:        fileIO.File,
			mappedBytes: bytes,
		}
	}
	return KeyValueLog{}
}

func (keyValueLog *KeyValueLog) Put(keyValuePair KeyValuePair) Offset {
	originalStartingOffset := keyValueLog.currentStartingOffset
	newStartingOffset := originalStartingOffset + keyValueLog.put(keyValuePair)
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

func createOrOpen(fileName string) *MutableFileIO {
	fileIO := NewFileIO()
	fileIO.CreateOrOpen(fileName)
	return fileIO
}

func (keyValueLog *KeyValueLog) put(keyValuePair KeyValuePair) Offset {
	fileIO := NewFileIO()

	fileIO.Open(keyValueLog.fileName, syscall.O_RDWR, 0600)
	offset := fileIO.WriteAt(keyValueLog.currentStartingOffset, keyValuePair.Serialize())
	return offset
}
