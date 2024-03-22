package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

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
func listAllMyBuckets(w http.ResponseWriter, r *http.Request, localPath string) (err error) {

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
