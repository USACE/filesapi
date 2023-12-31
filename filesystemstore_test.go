package filesapi

import (
	"errors"
	"fmt"
	"io"
	"os"
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
	//path := PathConfig{Path: "/Volumes/T7/Working/temp.tif"}
	path := PathConfig{Path: "asdfasdfasdf"}
	info, err := fs.GetObjectInfo(path)
	if err != nil {
		t.Fatal(err)
	}
	out := fmt.Sprintln(info)
	fmt.Println(out)
}

func TestFssFileExists(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := "/Volumes/T7/Working/temp.tif"
	fmt.Println(FileExists(fs, path))
	path = "asdfasdfasdf"
	fmt.Println(FileExists(fs, path))
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

func TestFssPutObjectByteSlice(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: "/Volumes/T7/Working/temp2.txt"}
	data := []byte("This is a test!")
	poi := PutObjectInput{
		Source: ObjectSource{
			Data: data,
		},
		Dest: path,
	}
	foo, err := fs.PutObject(poi)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(foo)
}

func TestFssPutObjectByFile(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	srcpath := PathConfig{Path: "/Volumes/T7/Working/temp2.txt"}
	destpath := PathConfig{Path: "/Volumes/T7/Working/temp3.txt"}
	poi := PutObjectInput{
		Source: ObjectSource{
			Filepath: srcpath,
		},
		Dest: destpath,
	}
	foo, err := fs.PutObject(poi)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(foo)
}

func TestFssPutObjectByReader(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	srcpath := PathConfig{Path: "/Volumes/T7/Working/temp2.txt"}
	f, err := os.OpenFile(srcpath.Path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	destpath := PathConfig{Path: "/Volumes/T7/Working/temp4.txt"}
	poi := PutObjectInput{
		Source: ObjectSource{
			Reader: f,
		},
		Dest: destpath,
	}
	foo, err := fs.PutObject(poi)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(foo)
}

func TestFssCopyObject(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	srcpath := PathConfig{Path: "/Volumes/T7/Working/temp4.txt"}
	destpath := PathConfig{Path: "/Volumes/T7/Working/temp5.txt"}
	coi := CopyObjectInput{
		Src:  srcpath,
		Dest: destpath,
	}
	err = fs.CopyObject(coi)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFssDeleteObjects(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	paths := PathConfig{Paths: []string{
		"/Volumes/T7/Working/temp4.txt",
		"/Volumes/T7/Working/temp5.txt",
	}}
	doi := DeleteObjectInput{
		Paths: paths,
	}
	fs.DeleteObjects(doi)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFssWalk(t *testing.T) {
	config := BlockFSConfig{}
	fs, err := NewFileStore(config)
	if err != nil {
		t.Fatal(err)
	}
	path := PathConfig{Path: "/Volumes/T7/Working/temp/ffrd"}
	wi := WalkInput{
		Path: path,
	}
	count := 0
	err = fs.Walk(wi, func(path string, fileinfo os.FileInfo) error {
		fmt.Println(path)
		if count > 10 {
			return errors.New("Too many!!!!")
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

/*
//initialize a multipart upload sessions
	InitializeObjectUpload(UploadConfig) (UploadResult, error)

	//write a chunk in a multipart upload session
	WriteChunk(UploadConfig) (UploadResult, error)

	//complete a multipart upload session
	CompleteObjectUpload(CompletedObjectUploadConfig) error

*/
