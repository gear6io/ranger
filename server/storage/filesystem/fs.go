package filesystem

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// FileStorage implements an in-memory file system for testing and CI
type FileStorage struct {
	files map[string]*file
	dirs  map[string]bool
	mu    sync.RWMutex
}

// memoryFile represents a file stored in memory
type file struct {
	data     []byte
	modTime  time.Time
	position int64
	mu       sync.RWMutex
}

// memoryWriteFile represents a file open for writing
type fileWriter struct {
	fs   *FileStorage
	path string
	buf  *bytes.Buffer
}

// memoryFileInfo implements os.FileInfo for memory files
type memoryFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

// NewFileStorage creates a new in-memory file system
func NewFileStorage() *FileStorage {
	return &FileStorage{
		files: make(map[string]*file),
		dirs:  make(map[string]bool),
	}
}

// Open opens a file for reading
func (mfs *FileStorage) Open(path string) (interface{}, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	cleanPath := filepath.Clean(path)
	fileData, exists := mfs.files[cleanPath]
	if !exists {
		return nil, &os.PathError{
			Op:   "open",
			Path: path,
			Err:  os.ErrNotExist,
		}
	}

	// Create a copy for reading
	fileCopy := &file{
		data:    make([]byte, len(fileData.data)),
		modTime: fileData.modTime,
	}
	copy(fileCopy.data, fileData.data)

	return &fileReader{
		file: fileCopy,
		path: path,
	}, nil
}

// Create creates a new file for writing
func (mfs *FileStorage) Create(path string) (interface{}, error) {
	cleanPath := filepath.Clean(path)

	// Ensure parent directories exist
	if err := mfs.ensureParentDirs(cleanPath); err != nil {
		return nil, err
	}

	return &fileWriter{
		fs:   mfs,
		path: cleanPath,
		buf:  bytes.NewBuffer(nil),
	}, nil
}

// Remove removes a file
func (mfs *FileStorage) Remove(path string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	cleanPath := filepath.Clean(path)
	if _, exists := mfs.files[cleanPath]; !exists {
		return &os.PathError{
			Op:   "remove",
			Path: path,
			Err:  os.ErrNotExist,
		}
	}

	delete(mfs.files, cleanPath)
	return nil
}

// Exists checks if a file or directory exists
func (mfs *FileStorage) Exists(path string) (bool, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	cleanPath := filepath.Clean(path)
	_, fileExists := mfs.files[cleanPath]
	dirExists := mfs.dirs[cleanPath]

	return fileExists || dirExists, nil
}

// Stat returns file information
func (mfs *FileStorage) Stat(path string) (os.FileInfo, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	cleanPath := filepath.Clean(path)

	// Check if it's a file
	if file, exists := mfs.files[cleanPath]; exists {
		return &memoryFileInfo{
			name:    filepath.Base(path),
			size:    int64(len(file.data)),
			modTime: file.modTime,
			isDir:   false,
		}, nil
	}

	// Check if it's a directory
	if mfs.dirs[cleanPath] {
		return &memoryFileInfo{
			name:    filepath.Base(path),
			size:    0,
			modTime: time.Now(),
			isDir:   true,
		}, nil
	}

	return nil, &os.PathError{
		Op:   "stat",
		Path: path,
		Err:  os.ErrNotExist,
	}
}

// ListDir lists directory contents
func (mfs *FileStorage) ListDir(path string) ([]os.FileInfo, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	cleanPath := filepath.Clean(path)
	if !mfs.dirs[cleanPath] {
		return nil, &os.PathError{
			Op:   "readdir",
			Path: path,
			Err:  os.ErrNotExist,
		}
	}

	var infos []os.FileInfo
	seen := make(map[string]bool)

	// Find all files and subdirectories in this path
	for filePath := range mfs.files {
		if strings.HasPrefix(filePath, cleanPath+"/") {
			relativePath := strings.TrimPrefix(filePath, cleanPath+"/")
			parts := strings.Split(relativePath, "/")
			if len(parts) > 0 {
				name := parts[0]
				if !seen[name] {
					seen[name] = true
					if len(parts) == 1 {
						// It's a file in this directory
						file := mfs.files[filePath]
						infos = append(infos, &memoryFileInfo{
							name:    name,
							size:    int64(len(file.data)),
							modTime: file.modTime,
							isDir:   false,
						})
					} else {
						// It's a subdirectory
						infos = append(infos, &memoryFileInfo{
							name:    name,
							size:    0,
							modTime: time.Now(),
							isDir:   true,
						})
					}
				}
			}
		}
	}

	// Sort by name
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Name() < infos[j].Name()
	})

	return infos, nil
}

// MkdirAll creates directories recursively
func (mfs *FileStorage) MkdirAll(path string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	cleanPath := filepath.Clean(path)
	// First ensure parent directories exist
	if err := mfs.ensureParentDirsLocked(cleanPath); err != nil {
		return err
	}
	// Then create the target directory itself
	mfs.dirs[cleanPath] = true
	return nil
}

// WriteFile writes data to a file (convenience method)
func (mfs *FileStorage) WriteFile(path string, data []byte) error {
	file, err := mfs.Create(path)
	if err != nil {
		return err
	}

	// The Create method always returns *fileWriter, so we can cast directly
	writeFile, ok := file.(*fileWriter)
	if !ok {
		return fmt.Errorf("unexpected file type: %T", file)
	}

	_, err = writeFile.Write(data)
	if err != nil {
		return err
	}

	// Close the writer to actually store the file
	return writeFile.Close()
}

// ReadFile reads data from a file (convenience method)
func (mfs *FileStorage) ReadFile(path string) ([]byte, error) {
	file, err := mfs.Open(path)
	if err != nil {
		return nil, err
	}

	// The Open method always returns *fileReader, so we can cast directly
	reader, ok := file.(*fileReader)
	if !ok {
		return nil, fmt.Errorf("unexpected file type: %T", file)
	}

	// Read all data from the reader
	data := make([]byte, 0)
	buffer := make([]byte, 1024)
	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			data = append(data, buffer[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

// Clear removes all files and directories
func (mfs *FileStorage) Clear() {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	mfs.files = make(map[string]*file)
	mfs.dirs = make(map[string]bool)
}

// OpenForRead opens a file for streaming read
func (mfs *FileStorage) OpenForRead(path string) (io.ReadCloser, error) {
	file, err := mfs.Open(path)
	if err != nil {
		return nil, err
	}

	// The Open method always returns *fileReader, so we can cast directly
	reader, ok := file.(*fileReader)
	if !ok {
		return nil, fmt.Errorf("unexpected file type: %T", file)
	}

	return reader, nil
}

// OpenForWrite opens a file for streaming write
func (mfs *FileStorage) OpenForWrite(path string) (io.WriteCloser, error) {
	file, err := mfs.Create(path)
	if err != nil {
		return nil, err
	}

	// The Create method always returns *fileWriter, so we can cast directly
	writer, ok := file.(*fileWriter)
	if !ok {
		return nil, fmt.Errorf("unexpected file type: %T", file)
	}

	return writer, nil
}

// ensureParentDirs ensures parent directories exist (with lock)
func (mfs *FileStorage) ensureParentDirs(path string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	return mfs.ensureParentDirsLocked(path)
}

// ensureParentDirsLocked ensures parent directories exist (assumes lock held)
func (mfs *FileStorage) ensureParentDirsLocked(path string) error {
	dir := filepath.Dir(path)

	// Termination conditions to prevent infinite recursion
	if dir == "." || dir == "/" || dir == "\\" || dir == path {
		return nil
	}

	// On Windows, check for drive root like "C:" or "C:\\"
	if len(dir) == 2 && dir[1] == ':' {
		return nil
	}
	if len(dir) == 3 && dir[1] == ':' && (dir[2] == '\\' || dir[2] == '/') {
		return nil
	}

	// Recursively ensure parent directories
	if err := mfs.ensureParentDirsLocked(dir); err != nil {
		return err
	}

	// Create this directory
	mfs.dirs[dir] = true
	return nil
}

// memoryReadFile implements io.File for reading
type fileReader struct {
	file *file
	path string
}

func (mrf *fileReader) Read(p []byte) (n int, err error) {
	mrf.file.mu.Lock()
	defer mrf.file.mu.Unlock()

	if mrf.file.position >= int64(len(mrf.file.data)) {
		return 0, io.EOF
	}

	n = copy(p, mrf.file.data[mrf.file.position:])
	mrf.file.position += int64(n)
	return n, nil
}

func (mrf *fileReader) ReadAt(p []byte, off int64) (n int, err error) {
	mrf.file.mu.RLock()
	defer mrf.file.mu.RUnlock()

	if off < 0 || off >= int64(len(mrf.file.data)) {
		return 0, io.EOF
	}

	n = copy(p, mrf.file.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return n, err
}

func (mrf *fileReader) Seek(offset int64, whence int) (int64, error) {
	mrf.file.mu.Lock()
	defer mrf.file.mu.Unlock()

	var newPos int64
	switch whence {
	case 0: // SEEK_SET
		newPos = offset
	case 1: // SEEK_CUR
		newPos = mrf.file.position + offset
	case 2: // SEEK_END
		newPos = int64(len(mrf.file.data)) + offset
	default:
		return 0, fmt.Errorf("invalid whence value: %d", whence)
	}

	if newPos < 0 {
		return 0, fmt.Errorf("negative position not allowed")
	}

	mrf.file.position = newPos
	return newPos, nil
}

func (mrf *fileReader) Stat() (os.FileInfo, error) {
	mrf.file.mu.RLock()
	defer mrf.file.mu.RUnlock()

	return &memoryFileInfo{
		name:    filepath.Base(mrf.path),
		size:    int64(len(mrf.file.data)),
		modTime: mrf.file.modTime,
		isDir:   false,
	}, nil
}

func (mrf *fileReader) Close() error {
	return nil
}

// memoryWriteFile methods
func (mwf *fileWriter) Write(p []byte) (n int, err error) {
	return mwf.buf.Write(p)
}

func (mwf *fileWriter) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read not supported on write-only file")
}

func (mwf *fileWriter) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("read not supported on write-only file")
}

func (mwf *fileWriter) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("seek not supported on write-only file")
}

func (mwf *fileWriter) Stat() (os.FileInfo, error) {
	return &memoryFileInfo{
		name:    filepath.Base(mwf.path),
		size:    int64(mwf.buf.Len()),
		modTime: time.Now(),
		isDir:   false,
	}, nil
}

func (mwf *fileWriter) Close() error {
	mwf.fs.mu.Lock()
	defer mwf.fs.mu.Unlock()

	// Store the file data
	mwf.fs.files[mwf.path] = &file{
		data:    mwf.buf.Bytes(),
		modTime: time.Now(),
	}

	return nil
}

// memoryFileInfo methods
func (mfi *memoryFileInfo) Name() string { return mfi.name }
func (mfi *memoryFileInfo) Size() int64  { return mfi.size }
func (mfi *memoryFileInfo) Mode() fs.FileMode {
	if mfi.isDir {
		return fs.ModeDir | 0755
	}
	return 0644
}
func (mfi *memoryFileInfo) ModTime() time.Time { return mfi.modTime }
func (mfi *memoryFileInfo) IsDir() bool        { return mfi.isDir }
func (mfi *memoryFileInfo) Sys() interface{}   { return nil }
