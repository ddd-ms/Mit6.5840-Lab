package mr

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func atomicWriteFile(filename string, r io.Reader) (err error) {
	dir, file := filepath.Split(filename)
	if dir == "" {
		dir = "."
	}
	// f,err := io.TempFile(dif,file)
	f, err := os.CreateTemp(dir, file)
	if err != nil {
		return fmt.Errorf("atomicWriteFile: %v", err)
	}
	defer func() {
		if err != nil {
			os.Remove(f.Name())
		} else {
			// ERRoR handle tempfile.
		}
	}()
	defer f.Close()

	name := f.Name()
	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("cant write data to tempfile %q, atomicWriteFile: %v", name, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cant close tempfile %q, atomicWriteFile: %v", name, err)
	}

	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		//no original file
	} else if err != nil {
		return err
	} else {
		if err := os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("cant set filemode on tempfile %q, atomicWriteFile: %v", name, err)
		}
	}
	if err := os.Rename(name, filename); err != nil {
		return fmt.Errorf("cant replace %q with tempfile %q, atomicWriteFile: %v", name, filename, err)
	}
	return nil
}
