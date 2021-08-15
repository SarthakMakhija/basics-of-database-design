package appendOnly

import (
	"syscall"
)

//TODO: Handle error gracefully
//TODO: Handle fileSize 4096

type Offset int64

type KeyValueLog struct {
	fileName              string
	logFileDescriptor     uintptr
	mappedBytes           []byte
	currentStartingOffset Offset
}

func NewKeyValueLog(fileName string) KeyValueLog {
	fileIO := createFile(fileName)
	bytes := fileIO.Mmap(fileIO.File, 4096)
	if fileIO.Err == nil {
		return KeyValueLog{
			fileName:          fileName,
			logFileDescriptor: fileIO.File.Fd(),
			mappedBytes:       bytes,
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

func (keyValueLog KeyValueLog) GetFirst() KeyValuePair {
	return keyValueLog.GetAtStartingOffset(0)
}

func (keyValueLog KeyValueLog) GetAtStartingOffset(offset Offset) KeyValuePair {
	return DeserializeFromOffset(keyValueLog.mappedBytes, offset)
}

func (keyValueLog *KeyValueLog) put(keyValuePair KeyValuePair) Offset {
	fileIO := NewFileIO()

	fileIO.Open(keyValueLog.fileName, syscall.O_RDWR, 0600)
	offset := fileIO.WriteAt(keyValueLog.currentStartingOffset, keyValuePair.Serialize())
	return offset
}

func createFile(fileName string) *MutableFileIO {
	fileIO := NewFileIO()
	fileIO.Create(fileName)
	return fileIO
}
