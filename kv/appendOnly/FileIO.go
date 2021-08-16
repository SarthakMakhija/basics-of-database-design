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

func (fileIO *MutableFileIO) CreateOrOpenReadWrite(fileName string) {
	fileIO.Open(fileName, os.O_RDWR|os.O_CREATE, 0600)
}

func (fileIO *MutableFileIO) OpenReadOnly(fileName string) {
	fileIO.Open(fileName, syscall.O_RDONLY, 0)
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

func (fileIO *MutableFileIO) Mmap(file *os.File, fileSizeInBytes int) ([]byte, bool) {
	if fileIO.Err != nil {
		return nil, false
	}
	resizeZeroSizedFile := func() bool {
		if fileIO.FileSize(file.Name()) == 0 {
			err := os.Truncate(file.Name(), int64(fileSizeInBytes))
			if err != nil {
				fileIO.Err = err
			}
			return true
		}
		return false
	}
	mmap := func() []byte {
		bytes, err := syscall.Mmap(int(file.Fd()), 0, fileSizeInBytes, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			fileIO.Err = err
			return nil
		}
		return bytes
	}
	isNew := resizeZeroSizedFile()
	return mmap(), isNew
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
