package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const (
	bucketPath   = "./buckets/" // Path where buckets will be stored
	s3user       = "s3user@amazon.com"
	userId       = "96f6d18b-4d8a-4b80-bfe0-0b6be6e663b6" // := uuid.New()
	storageClass = "STANDARD"
)

type BucketListResponse struct {
	Buckets []string `json:"buckets"`
}

func main() {
	// Create buckets directory if it doesn't exist
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		os.Mkdir(bucketPath, 0755)
	}

	// Set up routes
	http.HandleFunc("/", handleRequest)

	// Start server
	fmt.Println("S3 server is running on port 8080...")
	http.ListenAndServe(":8080", nil)
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleGetObject(w, r)
	case http.MethodPut:
		handlePutObject(w, r)
	case http.MethodDelete:
		handleDeleteObject(w, r)
	case http.MethodPost:
		handleCreateBucket(w, r)
	case "LIST":
		handleListBuckets(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleGetObject(w http.ResponseWriter, r *http.Request) {
	// Extract bucket name and object key from URL
	bucketName, objectKey := extractBucketAndKey(r.URL.Path, r.URL.RawQuery)

	if bucketName == "" {
		listAllMyBuckets(w, r, bucketPath)
		return
	}

	// Check if bucket exists
	bucketPath := filepath.Join(bucketPath, bucketName)
	fstat, err := os.Stat(bucketPath)

	if os.IsNotExist(err) {
		s3error(w, r, "The specified bucket does not exist", "NoSuchBucket", http.StatusNotFound)
		return
	}

	// Construct file path
	filePath := filepath.Join(bucketPath, objectKey)
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		s3error(w, r, "The resource you requested does not exist", "NoSuchKey", http.StatusNotFound)
		return
	}

	if fstat.IsDir() {
		listObjects(w, r, bucketPath, bucketName, objectKey)
		return
	}

	// Read file content
	fileContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		http.Error(w, "Failed to read object content", http.StatusInternalServerError)
		return
	}

	// Serve file content
	w.Header().Set("Content-Type", http.DetectContentType(fileContent))
	w.Write(fileContent)
}

func handlePutObject(w http.ResponseWriter, r *http.Request) {
	// Extract bucket name and object key from URL
	bucketName, objectKey := extractBucketAndKey(r.URL.Path, r.URL.RawQuery)

	// Check if bucket exists
	bucketPath := filepath.Join(bucketPath, bucketName)
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		http.Error(w, "Bucket not found", http.StatusNotFound)
		return
	}

	// Read object content from request body
	objectContent, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read object content", http.StatusInternalServerError)
		return
	}

	// Write object content to file
	filePath := filepath.Join(bucketPath, objectKey)
	err = ioutil.WriteFile(filePath, objectContent, 0644)
	if err != nil {
		http.Error(w, "Failed to write object to disk", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Object %s uploaded successfully to bucket %s", objectKey, bucketName)
}

func handleDeleteObject(w http.ResponseWriter, r *http.Request) {
	// Extract bucket name and object key from URL
	bucketName, objectKey := extractBucketAndKey(r.URL.Path, r.URL.RawQuery)

	// Check if bucket exists
	bucketPath := filepath.Join(bucketPath, bucketName)
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		http.Error(w, "Bucket not found", http.StatusNotFound)
		return
	}

	// Construct file path
	filePath := filepath.Join(bucketPath, objectKey)

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		http.Error(w, "Object not found", http.StatusNotFound)
		return
	}

	// Delete file
	err := os.Remove(filePath)
	if err != nil {
		http.Error(w, "Failed to delete object", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Object %s deleted successfully from bucket %s", objectKey, bucketName)
}

func handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	// Extract bucket name from URL
	bucketName := strings.TrimPrefix(r.URL.Path, "/")

	// Create bucket directory if it doesn't exist
	bucketPath := filepath.Join(bucketPath, bucketName)
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		err := os.Mkdir(bucketPath, 0755)
		if err != nil {
			http.Error(w, "Failed to create bucket", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintf(w, "Bucket %s created successfully", bucketName)
}

func handleListBuckets(w http.ResponseWriter, r *http.Request) {
	// List bucket directories
	files, err := ioutil.ReadDir(bucketPath)
	if err != nil {
		http.Error(w, "Failed to list buckets", http.StatusInternalServerError)
		return
	}

	// Extract bucket names
	bucketList := make([]string, 0)
	for _, file := range files {
		if file.IsDir() {
			bucketList = append(bucketList, file.Name())
		}
	}

	// Send bucket list as JSON response
	response := BucketListResponse{Buckets: bucketList}
	jsonData, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Failed to marshal bucket list", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func extractBucketAndKey(path string, rquery string) (string, string) {
	parts := strings.SplitN(path[1:], "/", 2)
	bucket := parts[0]
	key := ""

	if rquery != "" {
		tokens := strings.Split(rquery, "&")
		params := make(map[string]string)

		for _, arg := range tokens {
			//fmt.Println(arg)
			p := strings.Split(arg, "=")
			params[p[0]] = p[1]
		}
		key = strings.Replace(params["prefix"], params["delimiter"], "/", -1)

	}

	return bucket, key
}
