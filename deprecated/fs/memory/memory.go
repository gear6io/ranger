package memory

import (
	"bytes"
	"fmt"
	stdio "io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go/io"
)

// MemoryFileSystem implements an in-memory file system for testing and CI
type MemoryFileSystem struct {
	files map[string]*memoryFile
	dirs  map[string]bool
	mu    sync.RWMutex
}

// memoryFile represents a file stored in memory
type memoryFile struct {
	data     []byte
	modTime  time.Time
	position int64
	mu       sync.RWMutex
}

// memoryWriteFile represents a file open for writing
type memoryWriteFile struct {
	fs   *MemoryFileSystem
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

// NewMemoryFileSystem creates a new in-memory file system
func NewMemoryFileSystem() *MemoryFileSystem {
	return &MemoryFileSystem{
		files: make(map[string]*memoryFile),
		dirs:  make(map[string]bool),
	}
}

// Open opens a file for reading
func (mfs *MemoryFileSystem) Open(path string) (io.File, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	cleanPath := filepath.Clean(path)
	file, exists := mfs.files[cleanPath]
	if !exists {
		return nil, &os.PathError{
			Op:   "open",
			Path: path,
			Err:  os.ErrNotExist,
		}
	}

	// Create a copy for reading
	fileCopy := &memoryFile{
		data:    make([]byte, len(file.data)),
		modTime: file.modTime,
	}
	copy(fileCopy.data, file.data)

	return &memoryReadFile{
		file: fileCopy,
		path: path,
	}, nil
}

// Create creates a new file for writing
func (mfs *MemoryFileSystem) Create(path string) (io.File, error) {
	cleanPath := filepath.Clean(path)

	// Ensure parent directories exist
	if err := mfs.ensureParentDirs(cleanPath); err != nil {
		return nil, err
	}

	return &memoryWriteFile{
		fs:   mfs,
		path: cleanPath,
		buf:  bytes.NewBuffer(nil),
	}, nil
}

// Remove removes a file
func (mfs *MemoryFileSystem) Remove(path string) error {
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
func (mfs *MemoryFileSystem) Exists(path string) (bool, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	cleanPath := filepath.Clean(path)
	_, fileExists := mfs.files[cleanPath]
	dirExists := mfs.dirs[cleanPath]

	return fileExists || dirExists, nil
}

// Stat returns file information
func (mfs *MemoryFileSystem) Stat(path string) (os.FileInfo, error) {
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
func (mfs *MemoryFileSystem) ListDir(path string) ([]os.FileInfo, error) {
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
func (mfs *MemoryFileSystem) MkdirAll(path string, perm os.FileMode) error {
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
func (mfs *MemoryFileSystem) WriteFile(path string, data []byte) error {
	file, err := mfs.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Cast to our memory write file to access Write method
	if writeFile, ok := file.(*memoryWriteFile); ok {
		_, err = writeFile.Write(data)
		return err
	}

	return fmt.Errorf("file does not support writing")
}

// ReadFile reads data from a file (convenience method)
func (mfs *MemoryFileSystem) ReadFile(path string) ([]byte, error) {
	file, err := mfs.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	mfs.mu.RLock()
	cleanPath := filepath.Clean(path)
	memFile := mfs.files[cleanPath]
	mfs.mu.RUnlock()

	if memFile == nil {
		return nil, os.ErrNotExist
	}

	return memFile.data, nil
}

// Clear removes all files and directories
func (mfs *MemoryFileSystem) Clear() {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	mfs.files = make(map[string]*memoryFile)
	mfs.dirs = make(map[string]bool)
}

// ensureParentDirs ensures parent directories exist (with lock)
func (mfs *MemoryFileSystem) ensureParentDirs(path string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	return mfs.ensureParentDirsLocked(path)
}

// ensureParentDirsLocked ensures parent directories exist (assumes lock held)
func (mfs *MemoryFileSystem) ensureParentDirsLocked(path string) error {
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
type memoryReadFile struct {
	file *memoryFile
	path string
}

func (mrf *memoryReadFile) Read(p []byte) (n int, err error) {
	mrf.file.mu.Lock()
	defer mrf.file.mu.Unlock()

	if mrf.file.position >= int64(len(mrf.file.data)) {
		return 0, stdio.EOF
	}

	n = copy(p, mrf.file.data[mrf.file.position:])
	mrf.file.position += int64(n)
	return n, nil
}

func (mrf *memoryReadFile) ReadAt(p []byte, off int64) (n int, err error) {
	mrf.file.mu.RLock()
	defer mrf.file.mu.RUnlock()

	if off < 0 || off >= int64(len(mrf.file.data)) {
		return 0, stdio.EOF
	}

	n = copy(p, mrf.file.data[off:])
	if n < len(p) {
		err = stdio.EOF
	}
	return n, err
}

func (mrf *memoryReadFile) Seek(offset int64, whence int) (int64, error) {
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

func (mrf *memoryReadFile) Stat() (os.FileInfo, error) {
	mrf.file.mu.RLock()
	defer mrf.file.mu.RUnlock()

	return &memoryFileInfo{
		name:    filepath.Base(mrf.path),
		size:    int64(len(mrf.file.data)),
		modTime: mrf.file.modTime,
		isDir:   false,
	}, nil
}

func (mrf *memoryReadFile) Close() error {
	return nil
}

// memoryWriteFile methods
func (mwf *memoryWriteFile) Write(p []byte) (n int, err error) {
	return mwf.buf.Write(p)
}

func (mwf *memoryWriteFile) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("read not supported on write-only file")
}

func (mwf *memoryWriteFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("read not supported on write-only file")
}

func (mwf *memoryWriteFile) Seek(offset int64, whence int) (int64, error) {
	return 0, fmt.Errorf("seek not supported on write-only file")
}

func (mwf *memoryWriteFile) Stat() (os.FileInfo, error) {
	return &memoryFileInfo{
		name:    filepath.Base(mwf.path),
		size:    int64(mwf.buf.Len()),
		modTime: time.Now(),
		isDir:   false,
	}, nil
}

func (mwf *memoryWriteFile) Close() error {
	mwf.fs.mu.Lock()
	defer mwf.fs.mu.Unlock()

	// Store the file data
	mwf.fs.files[mwf.path] = &memoryFile{
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
