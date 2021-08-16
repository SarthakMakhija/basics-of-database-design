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
		return syscall.Flock(int(fileLock.LockedFile.Fd()), syscall.LOCK_UN)
	}
	return nil
}
