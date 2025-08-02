package memory

import (
	"io"

	icebergio "github.com/apache/iceberg-go/io"
)

// FileSystemAdapter adapts MemoryFileSystem to our FileSystem interface
type FileSystemAdapter struct {
	mfs *MemoryFileSystem
}

// NewFileSystemAdapter creates a new adapter for MemoryFileSystem
func NewFileSystemAdapter() (*FileSystemAdapter, error) {
	return &FileSystemAdapter{
		mfs: NewMemoryFileSystem(),
	}, nil
}

// Open opens a file for reading
func (fsa *FileSystemAdapter) Open(path string) (io.ReadCloser, error) {
	file, err := fsa.mfs.Open(path)
	if err != nil {
		return nil, err
	}
	return &fileReadCloser{file: file}, nil
}

// Create creates a new file for writing
func (fsa *FileSystemAdapter) Create(path string) (io.WriteCloser, error) {
	file, err := fsa.mfs.Create(path)
	if err != nil {
		return nil, err
	}

	// The Create method returns an icebergio.File, but we need to cast to memoryWriteFile
	// to access the Write method that extends the interface
	if writeFile, ok := file.(*memoryWriteFile); ok {
		return &fileWriteCloser{writeFile: writeFile}, nil
	}

	return nil, io.ErrShortWrite
}

// Remove removes a file
func (fsa *FileSystemAdapter) Remove(path string) error {
	return fsa.mfs.Remove(path)
}

// Exists checks if a file exists
func (fsa *FileSystemAdapter) Exists(path string) (bool, error) {
	return fsa.mfs.Exists(path)
}

// fileReadCloser adapts icebergio.File to io.ReadCloser
type fileReadCloser struct {
	file icebergio.File
}

func (frc *fileReadCloser) Read(p []byte) (n int, err error) {
	return frc.file.Read(p)
}

func (frc *fileReadCloser) Close() error {
	return frc.file.Close()
}

// fileWriteCloser adapts memoryWriteFile to io.WriteCloser
// memoryWriteFile extends icebergio.File with Write capability
type fileWriteCloser struct {
	writeFile *memoryWriteFile
}

func (fwc *fileWriteCloser) Write(p []byte) (n int, err error) {
	return fwc.writeFile.Write(p)
}

func (fwc *fileWriteCloser) Close() error {
	return fwc.writeFile.Close()
}
