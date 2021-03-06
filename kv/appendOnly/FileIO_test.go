package appendOnly_test

import (
	"bytes"
	"github.com/SarthakMakhija/basics-of-database-design/kv/appendOnly"
	"os"
	"testing"
)

func TestCreatesANewFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	fileIO.CreateOrOpenReadWrite(fileName)
	defer deleteFile(fileName)

	if fileIO.File.Name() != fileName {
		t.Fatalf("Expected file to be created with name %v but received %v", fileName, fileIO.File.Name())
	}
}

func TestCanNotCreatesANewFileGivenItIsADirectory(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "/"

	fileIO.CreateOrOpenReadWrite(fileName)
	defer deleteFile(fileName)

	if fileIO.Err == nil {
		t.Fatalf("Expected error to be found while creating a directory instead of file but received no error")
	}
}

func TestOpensANewFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)
	fileIO.CreateOrOpenReadWrite(fileName)
	fileIO.Open(fileName, os.O_RDWR, 0600)

	if fileIO.Err != nil {
		t.Fatalf("Expected not error to be found while opening a file but received %v", fileIO.Err)
	}
}

func TestDoesNotOpenANonExistentNewFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)
	fileIO.Open(fileName, os.O_RDWR, 0600)

	if fileIO.Err == nil {
		t.Fatalf("Expected error to be found while opening a non existent file but received no error")
	}
}

func TestMemoryMapsAFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.CreateOrOpenReadWrite(fileName)
	fileIO.Open(fileName, os.O_RDWR, 0600)
	content := []byte{'h', 'e', 'l', 'l', 'o'}
	_, _ = fileIO.File.WriteAt(content, 0)

	fileIO.Open(fileName, os.O_RDWR, 0400)
	mappedBytes, _ := fileIO.Mmap(fileIO.File, 5)

	if !bytes.Equal(content, mappedBytes) {
		t.Fatalf("Expected %v, received %v", content, mappedBytes)
	}
}

func TestResizesAFileOnMemoryMap(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.CreateOrOpenReadWrite(fileName)
	fileIO.Mmap(fileIO.File, 5)
	size := fileIO.FileSize(fileName)

	if size != 5 {
		t.Fatalf("Expected resized file to be of size %v, received %v", 5, size)
	}
}

func TestDoesNotMemoryMapANonExistentFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.Open(fileName, os.O_RDWR, 0600)
	mappedBytes, _ := fileIO.Mmap(fileIO.File, 5)

	if mappedBytes != nil {
		t.Fatalf("Expected %v, received %v", nil, mappedBytes)
	}
}

func TestUnMapsAFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.CreateOrOpenReadWrite(fileName)
	fileIO.Open(fileName, os.O_RDWR, 0600)
	content := []byte{'h', 'e', 'l', 'l', 'o'}
	_, _ = fileIO.File.WriteAt(content, 0)

	fileIO.Open(fileName, os.O_RDWR, 0400)
	mappedBytes, _ := fileIO.Mmap(fileIO.File, 5)
	fileIO.Munmap(mappedBytes)

	if fileIO.Err != nil {
		t.Fatalf("Expected no error while unmapping but received %v", fileIO.Err)
	}
}

func TestDoesNotUnMapANonExistentFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.Open(fileName, os.O_RDWR, 0600)
	fileIO.Munmap([]byte{'a', 'b'})

	if fileIO.Err == nil {
		t.Fatalf("Expected error while unmapping but received none")
	}
}

func TestReturnsTheFileSize(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.CreateOrOpenReadWrite(fileName)
	size := fileIO.FileSize(fileName)

	if size != 0 {
		t.Fatalf("Expected %v, received %v", 0, size)
	}
}

func TestDoesNotReturnTheFileSizeOfDirectory(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "/"

	defer deleteFile(fileName)

	fileIO.CreateOrOpenReadWrite(fileName)
	size := fileIO.FileSize(fileName)

	if size != -1 {
		t.Fatalf("Expected %v, received %v", -1, size)
	}
}

func TestOpensANewFileForReading(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)
	fileIO.CreateOrOpenReadWrite(fileName)
	fileIO.OpenReadOnly(fileName)

	if fileIO.Err != nil {
		t.Fatalf("Expected not error to be found while opening a file for reading but received %v", fileIO.Err)
	}
}

func TestDoesNotOpenANonExistentNewFileForReading(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)
	fileIO.OpenReadOnly(fileName)

	if fileIO.Err == nil {
		t.Fatalf("Expected error to be found while opening a non existent file for reading but received no error")
	}
}
