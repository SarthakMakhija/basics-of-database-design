package appendOnly

import (
	"os"
	"syscall"
)

type MutableFileIO struct {
	File *os.File
	Err  error
}

func NewFileIO() *MutableFileIO {
	return &MutableFileIO{}
}

func (fileIO *MutableFileIO) CreateOrOpen(fileName string) {
	file, err := os.OpenFile(fileName, os.O_RDONLY|os.O_CREATE, 0600)
	if err != nil {
		fileIO.Err = err
		return
	}
	fileIO.File = file
}

func (fileIO *MutableFileIO) Mmap(file *os.File, fileSizeInBytes int) []byte {
	if fileIO.Err != nil {
		return nil
	}
	bytes, err := syscall.Mmap(int(file.Fd()), 0, fileSizeInBytes, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		fileIO.Err = err
		return nil
	}
	return bytes
}

func (fileIO *MutableFileIO) Munmap(bytes []byte) {
	if fileIO.Err != nil {
		return
	}
	err := syscall.Munmap(bytes)
	if err != nil {
		fileIO.Err = err
	}
}

func (fileIO *MutableFileIO) WriteAt(offset Offset, bytes []byte) Offset {
	if fileIO.Err != nil {
		return -1
	}

	defer fileIO.CloseSilently()

	bytesWritten, err := fileIO.File.WriteAt(bytes, int64(offset))
	if err != nil {
		fileIO.Err = err
		return -1
	}
	return Offset(int64(bytesWritten))
}

func (fileIO *MutableFileIO) Open(fileName string, flag int, permission os.FileMode) {
	if fileIO.Err != nil {
		return
	}
	file, err := os.OpenFile(fileName, flag, permission)
	if err != nil {
		fileIO.Err = err
		return
	}
	fileIO.File = file
}

func (fileIO *MutableFileIO) OpenReadOnly(fileName string) {
	fileIO.Open(fileName, syscall.O_RDONLY, 0)
}

func (fileIO *MutableFileIO) FileSize(fileName string) int64 {
	if fileIO.Err != nil {
		return -1
	}
	stat, err := os.Stat(fileName)
	if err != nil {
		return -1
	}
	return stat.Size()
}

func (fileIO *MutableFileIO) CloseSilently() {
	_ = fileIO.File.Close()
}
