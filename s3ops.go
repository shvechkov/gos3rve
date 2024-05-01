package main

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func makeBucket(w http.ResponseWriter, r *http.Request, bucketName string) (err error) {

	// Check if bucket exists
	bucketPath := filepath.Join(bucketPath, bucketName)
	if _, err = os.Stat(bucketPath); os.IsNotExist(err) {
		if err = os.MkdirAll(bucketPath, 0755); err != nil {
			s3err(w, ErrInternalError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	return nil
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func listBuckets(w http.ResponseWriter, r *http.Request, localPath string) (err error) {

	f, err := os.Open(localPath)
	if err != nil {
		log.Println("Error opening directory:", err)
		return err
	}
	defer f.Close()

	files, err := f.Readdir(-1)
	if err != nil {
		log.Println("Error reading directory:", err)
		return err
	}

	// Responding with the list of buckets
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html

	var buffer bytes.Buffer

	buffer.WriteString(
		`<?xml version="1.0" encoding="UTF-8"?>  
<ListAllMyBucketsResult>
	<Buckets>
`)

	for _, file := range files {

		if file.IsDir() {
			mtime := file.ModTime().Format(time.RFC3339)
			entry := fmt.Sprintf("\t\t<Bucket><CreationDate>%s</CreationDate><Name>%s</Name></Bucket>\n", mtime, file.Name())
			buffer.WriteString(entry)
		}

	}

	buffer.WriteString(fmt.Sprintf(
		`
		</Buckets>
		<Owner>
			<DisplayName>%s</DisplayName>
			<ID>%s</ID>
		</Owner>
	</ListAllMyBucketsResult>
`, s3user, userId))

	w.Write(buffer.Bytes())
	return nil
}

// Define a struct to represent the XML structure
type XmlMultipartUploadPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type XmlCompleteMultipartUpload struct {
	Parts []XmlMultipartUploadPart `xml:"Part"`
}

func finilizeMultipartUpload(w http.ResponseWriter, r *http.Request, bucketPath string, objectKey string) error {

	_, _, uploadId, _ := isMultiPartUpload(r)

	objectContent, err := io.ReadAll(r.Body)
	if err != nil {
		s3err(w, ErrInternalError)
		log.Println("Error reading request data")
		return err
	}

	// Parse the XML data
	var data XmlCompleteMultipartUpload
	err = xml.Unmarshal(objectContent, &data)
	if err != nil {
		fmt.Println("Error parsing XML:", err)
		return err
	}

	dstFilePath := filepath.Join(bucketPath, objectKey)

	dstFile, err := os.OpenFile(dstFilePath, os.O_TRUNC|os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer dstFile.Close()

	for _, part := range data.Parts {
		srcFile := filepath.Dir(dstFilePath) + "/" + uploadId + "_" + strconv.FormatInt(int64(part.PartNumber), 10) + "_" + filepath.Base(dstFilePath)

		// Open the binary file for reading

		objectContent, err := os.ReadFile(srcFile)
		if err != nil {
			s3err(w, ErrInternalError)
			log.Printf("CompleteMultipartUpload: failed to read from  %s  rtt: %s", srcFile, err.Error())
			return err
		}

		//check  part's MD5
		hash := md5.New()
		if _, err = hash.Write(objectContent); err != nil {
			s3err(w, ErrInternalError)
			log.Println("Error while calculating md5 ", err.Error())
			return err
		}

		hash_str := hex.EncodeToString(hash.Sum(nil))
		if strings.Compare(part.ETag, hash_str) != 0 {
			s3err(w, ErrSignatureDoesNotMatch)
			log.Printf("CompleteMultipartUpload: part's signatures do not match (local: %s != client: %s) ",
				hash_str, part.ETag)

			return err

		}

		// Append data to the file
		_, err = dstFile.Write(objectContent)
		if err != nil {
			s3err(w, ErrInternalError)
			log.Println("CompleteMultipartUpload: Error while writing into file ", dstFilePath, " ", err.Error())
			return err
		}

		err = os.Remove(srcFile)
		if err != nil {
			log.Printf("CompleteMultipartUpload: Error wile deleting %s , err: %s", srcFile, err.Error())
		}

	}

	log.Printf("Multipart upload finished  for %s  (local path: %s ; object: %s)", r.URL.Path, bucketPath, objectKey)

	return nil
}

func putObject(w http.ResponseWriter, r *http.Request, path string, is_dir bool) (err error) {

	//request to create directory ?
	if is_dir {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			if err := os.Mkdir(path, 0755); err != nil {
				s3err(w, ErrInternalError)
				log.Printf("Error creating %s : %s\n", path, err)
				return err
			}
			w.WriteHeader(http.StatusCreated)
			return nil
		}

		fmt.Printf("Directory %s exists\n", path)
		w.WriteHeader(http.StatusConflict)
		return nil
	}

	//below goes a file upload request

	//if it is a multipart upload , modify filePath
	_, isMulti, uploadId, partNumber := isMultiPartUpload(r)
	if isMulti {
		path = filepath.Dir(path) + "/" + uploadId + "_" + partNumber + "_" + filepath.Base(path)
		//filePath = filePath + "_" + uploadId + "_" + partNumber
	}

	// make sure parent dir exists and create if it does not
	dirPath := filepath.Dir(path)
	if err = os.MkdirAll(dirPath, 0755); err != nil {
		s3err(w, ErrInternalError)
		log.Println("Error while creating parent directories")
		return
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		s3err(w, ErrInternalError)
		log.Printf("Error  creatig file %s", path)
		return
	}

	defer func() {
		file.Close()
		// If there was an error, delete file
		if err != nil {
			os.Remove(file.Name())
		}
	}()

	// Create a buffer to store chunks of data
	const MB = 1024 * 1024
	buffer := make([]byte, 5*MB)

	totalSize := 0
	n := 0

	hash := md5.New()

	// Loop to read the request body in chunks
	for {
		n, err = r.Body.Read(buffer)
		if err != nil && err != io.EOF {
			s3err(w, ErrInternalError)
			log.Println("Error reading request data")
			return err
		}

		// Check if we've reached the end of the request body
		if n == 0 {
			break
		}

		if _, err = hash.Write(buffer[:n]); err != nil {
			s3err(w, ErrInternalError)
			log.Println("Error while calculating md5 ", err.Error())
			return
		}

		// Write data to the file
		_, err = file.Write(buffer[:n])
		if err != nil {
			s3err(w, ErrInternalError)
			log.Printf("Error writing into file %s , err %s", path, err)
			return
		}

		// Increment the total size
		totalSize += n
	}

	hash_str := hex.EncodeToString(hash.Sum(nil))

	w.Header().Set("ETag", hash_str)
	w.WriteHeader(http.StatusCreated)

	return nil
}

func getObject(w http.ResponseWriter, r *http.Request, filePath string) error {

	// Check if file exists
	fstat, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		s3err(w, ErrNoSuchKey)
		return err
	}

	// Read file content
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		s3err(w, ErrInternalError)
		return err
	}

	hash := md5.New()
	if _, err = hash.Write(fileContent); err != nil {
		s3err(w, ErrInternalError)
		log.Println("Error while calculating md5 ", err.Error())
		return err
	}

	hash_str := hex.EncodeToString(hash.Sum(nil))

	w.Header().Set("ETag", hash_str)
	w.Header().Set("Content-Type", http.DetectContentType(fileContent))
	w.Header().Set("content-length", strconv.FormatInt(fstat.Size(), 10))
	w.Write(fileContent)

	return nil
}

func getObjectHead(w http.ResponseWriter, r *http.Request, filePath string) error {

	// Check if file exists
	fstat, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		s3err(w, ErrNoSuchKey)
		return err
	}

	// Read file content
	fileContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		http.Error(w, "Failed to read object content", http.StatusInternalServerError)
		return err
	}

	hash := md5.New()
	if _, err = hash.Write(fileContent); err != nil {
		s3err(w, ErrInternalError)
		log.Println("Error while calculating md5 ", err.Error())
		return err
	}

	hash_str := hex.EncodeToString(hash.Sum(nil))

	w.Header().Set("ETag", hash_str)
	w.Header().Set("Content-Type", http.DetectContentType(fileContent))
	w.Header().Set("content-length", strconv.FormatInt(fstat.Size(), 10))
	w.WriteHeader(http.StatusOK)

	return nil
}

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func listObjects(w http.ResponseWriter, r *http.Request, localPath string, bucketName string, objectKey string) (err error) {

	var buffer bytes.Buffer

	_, _, params := extractBucketAndKey(r)

	var s = fmt.Sprintf(`
	<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Name>%s</Name>
	<Prefix>%s</Prefix>
	<MaxKeys>1000</MaxKeys>
	<Marker/>
	<IsTruncated>false</IsTruncated>
	`, bucketName, params["prefix"])

	var common_prefixes strings.Builder
	buffer.WriteString(s)

	//	local_preffix := bucketPath + "/" + bucketName + "/"
	local_preffix := bucketPath + "/" + bucketName + "/"
	local_preffix = filepath.Clean(strings.TrimPrefix(local_preffix, "./"))

	// Open the directory
	path := localPath + "/" + objectKey
	d, err := os.Open(path)
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			// Check the specific error code
			if pathErr.Err == syscall.ENOTDIR {
				//ErrExistingObjectIsFile
				s3err(w, ErrExistingObjectIsFile)

				return
			}
		}

		s3err(w, ErrInternalError)
		log.Printf(" Can't open  %s  : %s", path, err.Error())
		return err
	}
	defer d.Close()

	info, err := d.Stat()
	if err != nil {
		s3err(w, ErrInternalError)
		log.Printf(" Canr stat  %s  : %s", path, err.Error())
		return err
	}

	var files []fs.DirEntry

	if !info.IsDir() {
		fileInfo, err := os.Stat(path)
		if err != nil {
			s3err(w, ErrInternalError)
			log.Printf(" Cant read stat %s  : %s", path, err.Error())
			return err
		}

		dirEntry := DirEntryFromStat(fileInfo)
		files = append(files, dirEntry)

		//we are dealing with "ls" on individual file
		//objectKey is treated as a dir name - chop off filename

		objectKey = filepath.Dir(objectKey)

	} else {
		// Read directory entries
		files, err = d.ReadDir(-1)
		if err != nil {
			s3err(w, ErrInternalError)
			log.Printf(" Cant read dir %s  : %s", path, err.Error())
			return err
		}

	}

	// Print the names of files in the directory
	for _, file := range files {
		info, _ := file.Info()

		fname := filepath.Clean(localPath + "/" + objectKey + "/" + file.Name())

		if !file.IsDir() {
			fname = strings.TrimPrefix(fname, local_preffix+"/")
			var entry = fmt.Sprintf(`
			<Contents>
				<Key>%s</Key>
				<LastModified>%s</LastModified>
				<Size>%d</Size>
				<StorageClass>%s</StorageClass>
				<Owner>
					<ID>%s</ID>
					<DisplayName>%s</DisplayName>
				</Owner>
			</Contents>
		
			`, EscapeStringForXML(fname), info.ModTime().Format(time.RFC3339), info.Size(), storageClass, userId, s3user)
			buffer.WriteString(entry)
		} else {

			fname = strings.TrimPrefix(fname, local_preffix)
			if fname != "" {
				var entry = fmt.Sprintf(`
				<CommonPrefixes>
					<Prefix>%s/</Prefix>
				</CommonPrefixes>
				`, EscapeStringForXML(fname))

				common_prefixes.WriteString(entry)
			}

		}

	} //ls dir

	buffer.WriteString(fmt.Sprintf(`	
				%s
			
			</ListBucketResult>
	`, common_prefixes.String()))

	w.Write(buffer.Bytes())
	return nil

}

func genBase64Str(len int) string {

	// Create a byte slice to hold the random bytes
	randomBytes := make([]byte, len)

	// Generate random bytes
	_, err := rand.Read(randomBytes)
	if err != nil {
		fmt.Println("Error generating random bytes:", err)
		return ""
	}

	// Encode random bytes as base64
	base64String := base64.StdEncoding.EncodeToString(randomBytes)

	// Trim the base64 string to the desired length
	base64String = base64String[:len]

	return base64String
}
