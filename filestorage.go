package deltalake

import (
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type ObjectStorage interface {
	Write(string, []byte) error
	List(string, string) ([]string, error)
	Read(string) (io.ReadCloser, error)
}

type fileStorage struct {
	dir string

	// todo: add cache since files are immutable
}

func newFileStorage(dir string) ObjectStorage {
	fStorage := &fileStorage{
		dir: dir,
	}
	fStorage.ensureDir()
	return fStorage
}

func (fs *fileStorage) ensureDir() {
	err := os.MkdirAll(fs.dir, os.ModePerm)
	if err != nil {
		slog.Error("error creating table dir", slog.Any("error", err))
	}
}

// write implements put-if-absent so the file will not be created if one already
// exists
func (fs *fileStorage) Write(file string, data []byte) error {
	f, err := os.OpenFile(fs.path(file), os.O_WRONLY|os.O_EXCL|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

// list returnrs the list of files whose name matches with prefix. If prefix is empty
// the list of files is just returned.
// Parameter subdir specifies subdirectory that should be searched. IF it is empty
// current directory for fs will be searched.
func (fs *fileStorage) List(subdir, pre string) ([]string, error) {
	root := func() string {
		if subdir == "" {
			return fs.dir
		}
		return path.Join(fs.dir, subdir)
	}()
	match := func(file string) bool {
		return pre == "" || strings.HasPrefix(file, pre)
	}
	fileNames := make([]string, 0)
	filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		name := path.Base(p)
		if match(name) {
			fileNames = append(fileNames, name)
		}
		return nil
	})
	return fileNames, nil
}

// read returns file instance, it is responsibilty of the caller to close the file
func (fs *fileStorage) Read(file string) (io.ReadCloser, error) {
	f, err := os.Open(fs.path(file))
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (fs *fileStorage) path(file string) string {
	return path.Join(fs.dir, file)
}
