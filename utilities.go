package filesapi

import (
	"archive/zip"
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	b64 "encoding/base64"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

const (
	signatureQueryName  string = "X-Amx-Signature"
	expirationQueryName string = "X-Amx-Expiration"
	timeQueryName       string = "X-Amx-Date"
	timeFormat          string = "20060102T150405Z"
	maxExpiration       int    = 86400 * 30 //30 days
)

type Retryer[T any] struct {

	//Max retry attempts
	MaxAttempts int

	//Max backoff in seconds
	MaxBackoff float64

	//base value for exponential backoff (usually 2)
	//https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html
	R float64
}

// Send function for platform agnostic retry with exponential backoff and jitter
// based on : https://docs.aws.amazon.com/sdkref/latest/guide/feature-retry-behavior.html
func (r Retryer[T]) Send(sendFunction func() (T, error)) (T, error) {
	attempts := 0
	for {
		t, err := sendFunction()
		if err == nil || attempts > r.MaxAttempts {
			return t, err
		}
		b := rand.Float64() //@TODO should probably use crypto random.....
		secondsToSleep := math.Min(b*math.Pow(r.R, float64(attempts)), r.MaxBackoff)
		time.Sleep(time.Second * time.Duration(secondsToSleep))
		attempts++
	}
}

type CountInput struct {

	//the filestore that will be walked
	FileStore FileStore

	//the starting directory
	DirPath PathConfig

	//an optional regular expression pattern for counting specific occurences of files
	Pattern string
}

// Counts the number of files matching an optional pattern.
// It accomplishes this by recursively walking the file system
// starting at the dirpath
func Count(ci CountInput) (int64, error) {
	var count int64 = 0
	var err error
	if ci.Pattern == "" {
		err = ci.FileStore.Walk(WalkInput{Path: ci.DirPath}, func(path string, file os.FileInfo) error {
			count++
			return nil
		})

	} else {
		var r *regexp.Regexp
		r, err := regexp.Compile("")
		if err != nil {
			return -1, fmt.Errorf("Failed to compile file search pattern: %s\n", err)
		}
		err = ci.FileStore.Walk(WalkInput{Path: ci.DirPath}, func(path string, file os.FileInfo) error {
			if r.MatchString(path) {
				count++
			}
			return nil
		})
	}

	if err != nil {
		return -1, err
	}
	return count, nil
}

type PresignInputOptions struct {

	//full uri, including query params, to sign or verify
	Uri string

	//HMAC 256 signing key
	SigningKey []byte

	//Expiration time in seconds
	Expiration int
}

// Signs a uri object.  Object should be a full uri with query parameters.
// method returns a new uri with the signature included.
// Signing parameter names are borrowed from AWS and use their spec:
// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
func PresignObject(options PresignInputOptions) (string, error) {
	if options.Expiration > maxExpiration {
		return "", errors.New("Expiration time too long")
	}
	uri, err := url.Parse(options.Uri)
	if err != nil {
		return "", err
	}
	qp := uri.Query()
	qp.Add(timeQueryName, time.Now().UTC().Format(timeFormat))
	qp.Add(expirationQueryName, strconv.Itoa(options.Expiration))
	uri.RawQuery = qp.Encode()
	signature, err := sign([]byte(uri.String()), options.SigningKey)
	sEnc := b64.StdEncoding.EncodeToString(signature)
	sUrl := url.QueryEscape(sEnc)
	qp.Add(signatureQueryName, sUrl)
	uri.RawQuery = qp.Encode()
	return uri.String(), nil
}

func sign(data []byte, signingKey []byte) ([]byte, error) {
	mac := hmac.New(sha256.New, signingKey)
	_, err := mac.Write(data)
	if err != nil {
		return nil, err
	}
	return mac.Sum(nil), nil
}

type UnZipInput struct {

	//the filestore that will be walked
	FileStore FileStore

	//the starting directory
	FilePath PathConfig
}

// UnZips Directory.
// client, err := s3.New("AKIAIWOWYYGZ2Y53UL3A", "wJalrXUtnFEMIK7MDENG/bPxRfiCYEXAMPLEKEY")
// if err != nil {
//   fmt.Println(err)
//   return
// }

// // Get the object from the bucket
// object, err := client.GetObject("my-bucket", "my-file.zip")
// if err != nil {
//   fmt.Println(err)
//   return
// }

// // Create a new zip reader
// reader, err := zip.NewReader(io.NopCloser(object))
// if err != nil {
//   fmt.Println(err)
//   return
// }

// // Iterate over the zip entries
// for _, entry := range reader.Entries {
//   // Get the entry name
//   name := entry.Name

//   // Create a new file
//   file, err := client.PutObject("my-bucket", name)
//   if err != nil {
// 	fmt.Println(err)
// 	return
//   }

//   // Write the entry contents to the file
//   _, err = file.Write(entry.Open())
//   if err != nil {
// 	fmt.Println(err)
// 	return
//   }
// }

// type unbufferedReaderAt struct {
// 	R io.Reader
// 	N int64
// }

// func NewUnbufferedReaderAt(r io.Reader) io.ReaderAt {
// 	return &unbufferedReaderAt{R: r}
// }

// func (u *unbufferedReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
// 	if off < u.N {
// 		return 0, errors.New("invalid offset")
// 	}
// 	diff := off - u.N
// 	written, err := io.CopyN(io.Discard, u.R, diff)
// 	u.N += written
// 	if err != nil {
// 		return 0, err
// 	}

// 	n, err = u.R.Read(p)
// 	u.N += int64(n)
// 	return
// }

// // Close the reader
// reader.Close()

// type PipeWriter struct {
// 	io.Writer
// }

// func (w PipeWriter) WriteAt(p []byte, offset int64) (n int, err error) {
// 	return w.Write(p)
// }

func UnZip(ui UnZipInput) error {
	// var err error
	// zipDir := filepath.Dir(ui.FilePath.Path)
	// object, err := ui.FileStore.GetObject(GetObjectInput{Path: ui.FilePath})
	// if err != nil {
	// 	return err
	// }
	// defer object.Close()
	// ura := NewUnbufferedReaderAt(object)
	// // reader, err := io.ReadAll(object)
	// // if err != nil {
	// // 	fmt.Println(err)
	// // 	return nil
	// // }
	// // // Create a new zip reader

	// z, err := zip.NewReader(ura, 0)
	// if err != nil {
	// 	fmt.Println(err)
	// 	return err
	// }
	// for _, entry := range z.File {
	// 	// Get the entry name

	// 	name := entry.Name
	// 	outPath := fmt.Sprintf("%s/%s", zipDir, name)
	// 	// Create a new file
	// 	file, err := ui.FileStore.PutObject(PutObjectInput{Source: ObjectSource{Reader: entry.ReaderVersion}, Dest: PathConfig{Path: outPath}})
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return nil
	// 	}

	// 	// Write the entry contents to the file
	// 	_, err = file.Write(entry.Open())
	// 	if err != nil {
	// 		fmt.Println(err)
	// 		return nil
	// 	}
	// }
	// return nil
	// pipeReader, pipeWriter := io.Pipe()

	// wg := sync.WaitGroup{}
	// wg.Add(2)
	// zipDir := filepath.Dir(ui.FilePath.Path)
	// go func() {
	// 	object, err := ui.FileStore.GetObject(GetObjectInput{Path: ui.FilePath})
	// 	io.Copy(pipeWriter, object)
	// 	//  downloader.Download(ctx, PipeWriter{pipeWriter}, &s3.GetObjectInput{
	// 	// 	Bucket: aws.String("input-bucket"),
	// 	// 	Key:    aws.String("mykey"),
	// 	// })
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	wg.Done()
	// 	pipeWriter.Close()
	// }()

	// go func() {
	// 	data := zipstream.NewReader(pipeReader)
	// 	for {
	// 		e, err := data.GetNextEntry()
	// 		if err != nil {
	// 			if err != io.EOF {
	// 				log.Fatal(err)
	// 			}
	// 			break
	// 		}
	// 		fmt.Printf("NewFile %s", e.Name)
	// 		rc, err := e.Open()
	// 		if err != nil {
	// 			log.Fatal("Failed to Open: ", e.Name)
	// 		}
	// 		log.Println("entry name: ", e.Name)
	// 		log.Println("entry comment: ", e.Comment)
	// 		log.Println("entry reader version: ", e.ReaderVersion)
	// 		log.Println("entry modify time: ", e.Modified)
	// 		log.Println("entry compressed size: ", e.CompressedSize64)
	// 		log.Println("entry uncompressed size: ", e.UncompressedSize64)
	// 		log.Println("entry is a dir: ", e.IsDir())
	// 		fmt.Printf("%s/%s", zipDir, e.Name)
	// 		_, err = ui.FileStore.PutObject(PutObjectInput{Source: ObjectSource{ContentLength: int64(e.UncompressedSize64), Reader: rc}, Dest: PathConfig{Path: fmt.Sprintf("%s/%s", zipDir, e.Name)}})

	// 		if err != nil {
	// 			fmt.Printf("Upload error %s \n", err)
	// 		}
	// 	}
	// 	wg.Done()
	// }()

	// wg.Wait()
	// object, err := ui.FileStore.GetObject(GetObjectInput{Path: ui.FilePath})
	// if err != nil {
	// 	log.Fatalf("Unable to Get Zip File: %s", err)
	// }
	// defer object.Close()

	// zr := zipstream.NewReader(object)

	// for {
	// 	e, err := zr.GetNextEntry()
	// 	if err == io.EOF {
	// 		break
	// 	}
	// 	if err != nil {
	// 		log.Fatalf("unable to get next entry: %s", err)
	// 	}

	// 	log.Println("entry name: ", e.Name)
	// 	log.Println("entry comment: ", e.Comment)
	// 	log.Println("entry reader version: ", e.ReaderVersion)
	// 	log.Println("entry modify time: ", e.Modified)
	// 	log.Println("entry compressed size: ", e.CompressedSize64)
	// 	log.Println("entry uncompressed size: ", e.UncompressedSize64)
	// 	log.Println("entry is a dir: ", e.IsDir())

	// 	if !e.IsDir() {
	// 		rc, err := e.Open()
	// 		if err != nil {
	// 			log.Printf("unable to open zip file: %s", err)
	// 		}
	// 		content, err := io.ReadAll(rc)
	// 		if err != nil {
	// 			log.Printf("read zip file content fail: %s", err)
	// 		}

	// 		log.Println("file length:", len(content))

	// 		if uint64(len(content)) != e.UncompressedSize64 {
	// 			log.Printf("read zip file length not equal with UncompressedSize64")
	// 		}
	// 		if err := rc.Close(); err != nil {
	// 			log.Printf("close zip entry reader fail: %s", err)
	// 		}
	// 	}
	// }
	// return nil
	zipDir := filepath.Dir(ui.FilePath.Path)
	fi, err := ui.FileStore.GetObjectInfo(PathConfig{Path: ui.FilePath.Path})
	if err != nil {
		log.Println(err)
	}
	size := int64(fi.Size())
	fmt.Printf("Zip Size %d \n", size)

	reader, err := zip.NewReader(ui.FileStore, size)
	if err != nil {
		log.Println(err)
	}
	for _, f := range reader.File {
		fmt.Printf("File %s \n", f.FileHeader.Name)
		reader, err := f.Open()
		if err != nil {
			log.Println(err)
		}
		_, err = ui.FileStore.PutObject(PutObjectInput{Source: ObjectSource{ContentLength: int64(f.FileHeader.UncompressedSize64), Reader: reader}, Dest: PathConfig{Path: fmt.Sprintf("%s/%s", zipDir, f.FileHeader.Name)}})
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}

// verify a signed object.  Returns a boolean.  True if verified, false on any error
// or validation failure
func VerifySignedObject(options PresignInputOptions) bool {
	uri, err := url.Parse(options.Uri)
	if err != nil {
		return false
	}
	sigok := verifySignature(uri, options.SigningKey)
	timeok := verifyExpiration(uri.Query())
	return sigok && timeok
}

func verifySignature(uri *url.URL, key []byte) bool {
	qp := uri.Query()
	urlSignature := qp.Get(signatureQueryName)
	b64signature, err := url.QueryUnescape(urlSignature)
	if err != nil {
		return false
	}
	signature, err := b64.StdEncoding.DecodeString(b64signature)
	if err != nil {
		return false
	}
	qp.Del(signatureQueryName)
	uri.RawQuery = qp.Encode()
	expectedSignature, err := sign([]byte(uri.String()), key)
	if err != nil {
		return false
	}
	return bytes.Equal(signature, expectedSignature)
}

func verifyExpiration(qp url.Values) bool {
	t, err := time.Parse(timeFormat, qp.Get(timeQueryName))
	if err != nil {
		return false
	}
	d, err := strconv.Atoi(qp.Get(expirationQueryName))
	if err != nil {
		return false
	}
	t = t.Add(time.Second * time.Duration(d))
	return t.After(time.Now().UTC())

}

type ZipInput struct {

	//the filestore that will be walked
	FileStore FileStore

	//the starting directory
	DirPath PathConfig
}

// Zips up Directory.
// It accomplishes this by recursively walking the file system
// starting at the dirpath
func Zip(zi ZipInput) (int64, error) {
	var count int64 = 0
	var err error

	err = zi.FileStore.Walk(WalkInput{Path: zi.DirPath}, func(path string, file os.FileInfo) error {
		count++
		return nil
	})

	if err != nil {
		return -1, err
	}
	return count, nil
}
