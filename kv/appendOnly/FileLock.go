package appendOnly

import (
	"os"
	"syscall"
)

type FileLock struct {
	LockedFile *os.File
	Err        error
}

func AcquireExclusiveLock(fileName string) FileLock {
	fileIO := NewFileIO()
	fileIO.OpenReadOnly(fileName)

	lockMode := syscall.LOCK_EX | syscall.LOCK_NB
	err := syscall.Flock(int(fileIO.File.Fd()), lockMode)

	if err != nil {
		return FileLock{
			Err: err,
		}
	}
	return FileLock{
		LockedFile: fileIO.File,
	}
}

func (fileLock FileLock) Release() error {
	if fileLock.LockedFile != nil {
		err := fileLock.unlock()
		if err != nil {
			return err
		}
		fileLock.close()
	}
	return nil
}

func (fileLock FileLock) unlock() error {
	return syscall.Flock(int(fileLock.LockedFile.Fd()), syscall.LOCK_UN)
}

func (fileLock FileLock) close() {
	fileIO := NewFileIO()
	fileIO.File = fileLock.LockedFile
	fileIO.CloseSilently()
}
