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

	fileIO.Create(fileName)
	defer deleteFile(fileName)

	if fileIO.File.Name() != fileName {
		t.Fatalf("Expected file to be created with name %v but received %v", fileName, fileIO.File.Name())
	}
}

func TestCanNotCreatesANewFileGivenItIsADirectory(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "/usr"

	fileIO.Create(fileName)
	defer deleteFile(fileName)

	if fileIO.Err == nil {
		t.Fatalf("Expected error to be found while creating a directory instead of file but received no error")
	}
}

func TestOpensANewFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)
	fileIO.Create(fileName)
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

func TestWritesAtAnOffsetInAFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.Create(fileName)
	fileIO.Open(fileName, os.O_RDWR, 0600)
	content := []byte{'h', 'e', 'l', 'l', 'o'}
	fileIO.WriteAt(0, content)

	bytesRead := make([]byte, 5)
	fileIO.Open(fileName, os.O_RDWR, 0600)
	_, _ = fileIO.File.Read(bytesRead)

	if !bytes.Equal(content, bytesRead) {
		t.Fatalf("Expected %v, received %v", content, bytesRead)
	}
}

func TestDoesNotWriteToANonExistentFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.Open(fileName, os.O_RDWR, 0600)
	content := []byte{'h', 'e', 'l', 'l', 'o'}
	offset := fileIO.WriteAt(0, content)

	if offset != -1 {
		t.Fatalf("Expected %v, received %v", -1, offset)
	}
}

func TestMemoryMapsAFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.Create(fileName)
	fileIO.Open(fileName, os.O_RDWR, 0600)
	content := []byte{'h', 'e', 'l', 'l', 'o'}
	fileIO.WriteAt(0, content)

	fileIO.Open(fileName, os.O_RDONLY, 0400)
	mappedBytes := fileIO.Mmap(fileIO.File, 5)

	if !bytes.Equal(content, mappedBytes) {
		t.Fatalf("Expected %v, received %v", content, mappedBytes)
	}
}

func TestDoesNotMemoryMapANonExistentFile(t *testing.T) {
	fileIO := appendOnly.NewFileIO()
	fileName := "./kv.test"

	defer deleteFile(fileName)

	fileIO.Open(fileName, os.O_RDWR, 0600)
	mappedBytes := fileIO.Mmap(fileIO.File, 5)

	if mappedBytes != nil {
		t.Fatalf("Expected %v, received %v", nil, mappedBytes)
	}
}
