package appendOnly

import (
	"os"
	"syscall"
)

type FileIO struct {
	file *os.File
	err  error
}

func NewFileIO() *FileIO {
	return &FileIO{}
}

func (fileIO *FileIO) Create(fileName string) {
	file, err := os.Create(fileName)
	if err != nil {
		fileIO.err = err
		return
	}
	fileIO.file = file
}

func (fileIO *FileIO) Open(fileName string, flag int, permission os.FileMode) {
	if fileIO.err != nil {
		return
	}
	file, err := os.OpenFile(fileName, flag, permission)
	if err != nil {
		fileIO.err = err
		return
	}
	fileIO.file = file
}

func (fileIO *FileIO) Mmap(file *os.File, fileSize int) []byte {
	if fileIO.err != nil {
		return nil
	}
	bytes, err := syscall.Mmap(int(file.Fd()), 0, fileSize, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		fileIO.err = err
		return nil
	}
	return bytes
}

func (fileIO *FileIO) WriteAt(offset Offset, bytes []byte) Offset {
	if fileIO.err != nil {
		return 0
	}

	defer syscall.Close(int(fileIO.file.Fd()))

	bytesWritten, err := fileIO.file.WriteAt(bytes, int64(offset))
	if err != nil {
		fileIO.err = err
		return -1
	}
	return Offset(int64(bytesWritten))
}
