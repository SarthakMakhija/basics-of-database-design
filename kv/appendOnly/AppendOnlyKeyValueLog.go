package appendOnly

import (
	"fmt"
	"os"
	"syscall"
)

//TODO: Handle error gracefully
//TODO: Handle fileSize 4096

type KeyValueLog struct {
	fileName              string
	logFileDescriptor     uintptr
	mappedBytes           []byte
	currentStartingOffset int64
}

func NewKeyValueLog(fileName string) KeyValueLog {
	file, err := createFile(fileName)
	if err == nil {
		bytes, err := mMap(file, 4096)
		if err == nil {
			return KeyValueLog{
				fileName:          fileName,
				logFileDescriptor: file.Fd(),
				mappedBytes:       bytes,
			}
		} else {
			fmt.Print(err)
			panic("handle later")
		}
	} else {
		fmt.Print(err)
		panic("handle later")
	}
}

func (keyValueLog *KeyValueLog) Put(keyValuePair KeyValuePair) int64 {
	originalStartingOffset := keyValueLog.currentStartingOffset
	newStartingOffset := originalStartingOffset + int64(keyValueLog.put(keyValuePair))
	keyValueLog.currentStartingOffset = newStartingOffset

	return originalStartingOffset
}

func (keyValueLog KeyValueLog) GetFirst() KeyValuePair {
	return keyValueLog.GetAtStartingOffset(0)
}

func (keyValueLog KeyValueLog) GetAtStartingOffset(offset int64) KeyValuePair {
	return DeserializeFromOffset(keyValueLog.mappedBytes, offset)
}

func (keyValueLog *KeyValueLog) put(keyValuePair KeyValuePair) int {
	file, err := os.OpenFile(keyValueLog.fileName, syscall.O_RDWR, 0600)
	defer syscall.Close(int(file.Fd()))

	if err == nil {
		bytesWritten, err := file.WriteAt(keyValuePair.Serialize(), keyValueLog.currentStartingOffset)
		if err == nil {
			return bytesWritten
		}
	}
	return 0
}

func createFile(fileName string) (*os.File, error) {
	return os.Create(fileName)
}

func mMap(file *os.File, fileSize int) ([]byte, error) {
	bytes, err := syscall.Mmap(int(file.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
