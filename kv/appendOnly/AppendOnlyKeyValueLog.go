package appendOnly

import (
	"gitlab.com/stone.code/assert"
	"os"
	"sync/atomic"
)

type Offset uint64

func (offset Offset) toUint64() uint64 {
	return uint64(offset)
}

type KeyValueLog struct {
	fileName              string
	file                  *os.File
	mappedBytes           []byte
	currentStartingOffset uint64
}

func NewKeyValueLog(fileName string) KeyValueLog {
	newKeyValueLog := func(file *os.File, mappedBytes []byte) KeyValueLog {
		return KeyValueLog{
			fileName:    fileName,
			file:        file,
			mappedBytes: mappedBytes,
		}
	}
	nextStartingOffset := func(bytes []byte) Offset {
		return NewKeyValuePairIterator(bytes).NextStartingOffset()
	}

	fileIO := CreateOrOpenReadWrite(fileName)
	bytes, isNew := fileIO.Mmap(fileIO.File, 4096)
	if fileIO.Err == nil {
		log := newKeyValueLog(fileIO.File, bytes)
		if !isNew {
			log.currentStartingOffset = nextStartingOffset(bytes).toUint64()
		}
		return log
	}
	return KeyValueLog{}
}

func (keyValueLog *KeyValueLog) Put(keyValuePair KeyValuePair) Offset {
	serializedContent := func() ([]byte, uint64) {
		content := keyValuePair.Serialize()
		return content, uint64(len(content))
	}
	atomicIncrementOffSetBy := func(size uint64) uint64 {
		endOffset := atomic.AddUint64(&keyValueLog.currentStartingOffset, size)
		return endOffset
	}
	startingOffset := func(endOffset uint64, serializedContentSize uint64) uint64 {
		return endOffset - serializedContentSize
	}
	put := func() uint64 {
		bytes, size := serializedContent()
		startingOffset := startingOffset(atomicIncrementOffSetBy(size), size)
		copy(keyValueLog.mappedBytes[startingOffset:], bytes)
		return startingOffset
	}
	return Offset(put())
}

func (keyValueLog KeyValueLog) GetAtStartingOffset(offset Offset) KeyValuePair {
	return NewKeyValuePairIteratorAt(offset, keyValueLog.mappedBytes).Next()
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
