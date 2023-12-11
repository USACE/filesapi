package filesapi

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestFssGetDir(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: "/Volumes/T7/Working"}
	dirs, err := fs.GetDir(path)
	if err != nil {
		t.Fatal(err)
	}
	out := fmt.Sprintln(dirs)
	fmt.Println(out)
}

func TestFssGetObjectInfo(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: "/Volumes/T7/Working/temp.tif"}
	info, err := fs.GetObjectInfo(path)
	if err != nil {
		t.Fatal(err)
	}
	out := fmt.Sprintln(info)
	fmt.Println(out)
}

func TestFssGetObject(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}

	goi := GetObjectInput{
		Path: PathConfig{Path: "/Volumes/T7/Working/test.json"},
	}
	reader, err := fs.GetObject(goi)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	buf := new(strings.Builder)
	_, err = io.Copy(buf, reader)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(buf.String())
	fmt.Println("------------------------------------------")

	goi.Range = "bytes=0-20"
	reader, err = fs.GetObject(goi)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	buf = new(strings.Builder)
	_, err = io.Copy(buf, reader)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(buf.String())

}
