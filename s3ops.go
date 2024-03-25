package main

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func makeBucket(w http.ResponseWriter, r *http.Request, bucketName string) (err error) {

	// Check if bucket exists
	bucketPath := filepath.Join(bucketPath, bucketName)
	if _, err = os.Stat(bucketPath); os.IsNotExist(err) {
		if err = os.MkdirAll(bucketPath, 0755); err != nil {
			s3error(w, r, "InternalServerError", "InternalServerError", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	return nil
}

// consult with this
// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#RESTErrorResponses
func s3error(w http.ResponseWriter, r *http.Request, msg string, code string, httpStatus int) (err error) {

	var buffer bytes.Buffer

	buffer.WriteString(fmt.Sprintf(
		`<?xml version='1.0' encoding='UTF-8'?>
		<Error>
			<Code>%s</Code>
			<Message>%s</Message>
			<RequestId>4442587FB7D0A2F9</RequestId>
		</Error>
	
`, code, msg))

	http.Error(w, buffer.String(), httpStatus)

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

	objectContent, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s3error(w, r, "InternalServerError", "InternalServerError", http.StatusInternalServerError)
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
		fmt.Printf("PartNumber: %d, ETag: %s\n", part.PartNumber, part.ETag)

		srcFile := bucketPath + "/" + uploadId + "_" + strconv.FormatInt(int64(part.PartNumber), 10) + "_" + objectKey
		// Open the binary file for reading

		objectContent, err := os.ReadFile(srcFile)
		if err != nil {
			s3error(w, r, "InternalServerError", "InternalServerError", http.StatusInternalServerError)
			log.Println("CompleteMultipartUpload: failed to read from ", srcFile, " ", err.Error())
			return err

		}
		//TBD - check MD5s for parts
		// !!!!!!!!!

		// Append data to the file
		_, err = dstFile.Write(objectContent)
		if err != nil {
			s3error(w, r, "InternalServerError", "InternalServerError", http.StatusInternalServerError)
			log.Println("CompleteMultipartUpload: Error while writing into file ", dstFilePath, " ", err.Error())
			return err
		}

		err = os.Remove(srcFile)
		if err != nil {
			log.Printf("CompleteMultipartUpload: Error wile deleting %s , err: %s", srcFile, err.Error())
		}

	}

	return nil
}

func putObject(w http.ResponseWriter, r *http.Request, filePath string) (err error) {

	// Read object content from request body
	objectContent, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s3error(w, r, "InternalServerError", "InternalServerError", http.StatusInternalServerError)
		log.Println("Error reading request data")
		return
	}

	// Write object content to file
	dirPath := filepath.Dir(filePath)
	if err = os.MkdirAll(dirPath, 0755); err != nil {
		s3error(w, r, "InternalServerError", "InternalServerError", http.StatusInternalServerError)
		log.Println("Error while creating parent directories")
		return
	}

	//if it is a multipart upload , modify filePath
	err, isMulti, uploadId, partNumber := isMultiPartUpload(r)
	if isMulti {
		filePath = filepath.Dir(filePath) + "/" + uploadId + "_" + partNumber + "_" + filepath.Base(filePath)
		//filePath = filePath + "_" + uploadId + "_" + partNumber
	}

	if err = os.WriteFile(filePath, objectContent, 0644); err != nil {
		s3error(w, r, "InternalServerError", "InternalServerError", http.StatusInternalServerError)
		log.Println("Error while writing into file ", filePath, " ", err.Error())
		return
	}

	hash := md5.New()
	if _, err = hash.Write(objectContent); err != nil {
		s3error(w, r, "InternalServerError", "InternalServerError", http.StatusInternalServerError)
		log.Println("Error while calculating md5 ", err.Error())
		return
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
		s3error(w, r, "The resource you requested does not exist", "NoSuchKey", http.StatusNotFound)
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
		s3error(w, r, "InternalServerError", "InternalServerError", http.StatusInternalServerError)
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

// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
func listObjects(w http.ResponseWriter, r *http.Request, localPath string, bucketName string, objectKey string) (err error) {

	var buffer bytes.Buffer

	var s = fmt.Sprintf(`
	<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Name>%s</Name>
	<Prefix/>
	<MaxKeys>1000</MaxKeys>
	<Marker/>
	<IsTruncated>false</IsTruncated>
	`, bucketName)

	buffer.WriteString(s)

	err = filepath.Walk(localPath+"/"+objectKey,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {

				fname := strings.TrimPrefix(path, strings.TrimPrefix(bucketPath+bucketName+"/", "./"))
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
			
				`, fname, info.ModTime().Format(time.RFC3339), info.Size(), storageClass, userId, s3user)
				buffer.WriteString(entry)

			}
			return nil
		})

	buffer.WriteString(
		`
	</ListBucketResult>
`)

	w.Write(buffer.Bytes())
	return nil

}
