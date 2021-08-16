package appendOnly_test

import (
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"testing"
)

func release(fileLock appendOnly.FileLock) {
	_ = fileLock.Release()
}

func TestAcquiresFileLock(t *testing.T) {
	fileName := "./kv.test"

	fileIO := appendOnly.NewFileIO()
	fileIO.CreateOrOpen(fileName)

	fileLock := appendOnly.AcquireExclusiveLock(fileName)
	defer release(fileLock)

	if fileLock.Err != nil {
		t.Fatalf("Expected lock to be acquired on the file but received an error %v", fileLock.Err)
	}
}

func TestOneCallAcquiresAnExclusiveLockAndOtherFails(t *testing.T) {
	fileName := "./kv.test"

	fileIO := appendOnly.NewFileIO()
	fileIO.CreateOrOpen(fileName)

	fileLock1 := appendOnly.AcquireExclusiveLock(fileName)
	fileLock2 := appendOnly.AcquireExclusiveLock(fileName)

	defer release(fileLock1)

	if fileLock1.Err != nil {
		t.Fatalf("Expected lock to be acquired by the first process but received an error %v", fileLock1.Err)
	}
	if fileLock2.Err == nil {
		t.Fatalf("Expected lock to not be acquired by the first process but received no error")
	}
}

func TestReleasesALockOnTheFile(t *testing.T) {
	fileName := "./kv.test"

	fileIO := appendOnly.NewFileIO()
	fileIO.CreateOrOpen(fileName)

	fileLock := appendOnly.AcquireExclusiveLock(fileName)
	err := fileLock.Release()

	if err != nil {
		t.Fatalf("Expected lock to be released successfully but received an error %v", err)
	}
}
