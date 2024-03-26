package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	bucketPath         = "./buckets/" // Path where buckets will be stored
	uploadsPath        = "./uploads/" // Path where to store multipaert upload parts before assembling
	s3user             = "s3user@amazon.com"
	userId             = "96f6d18b-4d8a-4b80-bfe0-0b6be6e663b6" // := uuid.New()
	storageClass       = "STANDARD"
	keyId              = ""
	secretKey          = ""
	svcPort      int64 = 8080
)

type BucketListResponse struct {
	Buckets []string `json:"buckets"`
}

func main() {

	// Define flags with default values and descriptions
	flag.Int64Var(&svcPort, "p", 8080, "Port to listen on")
	flag.StringVar(&uploadsPath, "dir_uploads", "./uploads/", "temp dir to store upload parts")
	flag.StringVar(&bucketPath, "dir_buckets", "./buckets/", "dir to store buckets")
	flag.StringVar(&s3user, "user_name", "s3user@amazon.com", "AWS S3 user name")
	flag.StringVar(&userId, "user_id", "96f6d18b-4d8a-4b80-bfe0-0b6be6e663b6", "AWS S3 user ID")
	flag.StringVar(&keyId, "key_id", genBase64Str(10), "Access Key ID")
	flag.StringVar(&secretKey, "key_val", genBase64Str(32), "Secret Access Key")
	help := flag.Bool("h", false, "Show usage")

	flag.Parse()
	if *help {
		flag.Usage()
		return
	}

	// Create buckets directory if it doesn't exist
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		os.Mkdir(bucketPath, 0755)
	}

	// Create uploads directory if it doesn't exist
	if _, err := os.Stat(uploadsPath); os.IsNotExist(err) {
		os.Mkdir(uploadsPath, 0755)
	}

	// Set up routes
	http.HandleFunc("/", handleRequest)

	// Start server
	log.Printf("S3 server is running on port %d ...", svcPort)
	log.Printf("uploads dir  %s ...", uploadsPath)
	log.Printf("buckets dir  %s ...", bucketPath)
	log.Printf("access key id  %s ...", keyId)
	log.Printf("secret access key %s ...", secretKey)

	http.ListenAndServe(":"+strconv.FormatInt(svcPort, 10), nil)
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleGetRequest(w, r)
	case http.MethodPut:
		handlePutRequest(w, r)
	case http.MethodDelete:
		handleDeleteRequest(w, r)
	case http.MethodPost:
		handlePostRequest(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleGetRequest(w http.ResponseWriter, r *http.Request) {
	// Extract bucket name and object key from URL
	bucketName, objectKey, params := extractBucketAndKey(r)

	if bucketName == "" {
		_ = listBuckets(w, r, bucketPath)
		return
	}

	// Check if bucket exists
	bucketPath := filepath.Join(bucketPath, bucketName)
	_, err := os.Stat(bucketPath)
	if os.IsNotExist(err) {
		s3error(w, r, "The specified bucket does not exist", "NoSuchBucket", http.StatusNotFound)
		return
	}

	// Construct file path
	filePath := filepath.Join(bucketPath, objectKey)
	// Check if file exists
	fstat, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		s3error(w, r, "The resource you requested does not exist", "NoSuchKey", http.StatusNotFound)
		return
	}

	//If key/prefix  points to a dir -> return list of objects with a given prefix
	if fstat.IsDir() || (params["prefix"] != "") {
		listObjects(w, r, bucketPath, bucketName, objectKey)
		return
	}

	getObject(w, r, filePath)
}

func handlePutRequest(w http.ResponseWriter, r *http.Request) {
	// Extract bucket name and object key from URL
	bucketName, objectKey, _ := extractBucketAndKey(r)

	//Create Bucket request  -  PUT with bucket name and w/o object
	if bucketName != "" && objectKey == "" {
		makeBucket(w, r, bucketName)
		return
	}

	// Handling below the request to create/upload an opbject

	// Check if bucket exists
	bucketPath := filepath.Join(bucketPath, bucketName)
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		s3error(w, r, "The specified bucket does not exist", "NoSuchBucket", http.StatusNotFound)
		return
	}

	// Write object content to file
	filePath := filepath.Join(bucketPath, objectKey)
	putObject(w, r, filePath)
}

func handleDeleteRequest(w http.ResponseWriter, r *http.Request) {
	// Extract bucket name and object key from URL
	bucketName, objectKey, _ := extractBucketAndKey(r)

	// Check if bucket exists
	bucketPath := filepath.Join(bucketPath, bucketName)
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
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

	// Delete bucket/object
	err := os.Remove(filePath)
	if err != nil {
		//TBD  - we most prob got here because dir is not empty ..Provide beter err handling/reporting
		s3error(w, r, "Bucket is not empty", "NotEmpty", http.StatusConflict)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handlePostRequest(w http.ResponseWriter, r *http.Request) {

	// Extract bucket name and object key from URL
	bucketName, objectKey, _ := extractBucketAndKey(r)

	// Check if bucket exists
	bucketPath := filepath.Join(bucketPath, bucketName)
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		s3error(w, r, "The specified bucket does not exist", "NoSuchBucket", http.StatusNotFound)
		return
	}

	//Logics for handling Multipart uploads goes below

	//CreateMultipartUpload
	//https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
	if r.URL.RawQuery == "uploads" {

		var buffer bytes.Buffer
		buffer.WriteString(
			fmt.Sprintf(`
			<InitiateMultipartUploadResult>
				<Bucket>%s</Bucket>
				<Key>%s</Key>
				<UploadId>%s</UploadId>
			</InitiateMultipartUploadResult>
	`, bucketName, objectKey, strconv.FormatInt(time.Now().UnixNano(), 10)))

		w.WriteHeader(http.StatusOK)
		w.Write(buffer.Bytes())
		log.Printf("Multipart upload intiated for %s  (bucket: %s ; object: %s)", r.URL.Path, bucketName, objectKey)
		return
	}

	//https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
	//CompleteMultipartUpload
	pattern := `^uploadId=\d+$`
	regexFinilizeUpload, err := regexp.Compile(pattern)
	if err != nil {
		fmt.Println("Error compiling regular expression:", err)
		return
	}

	if regexFinilizeUpload.MatchString(r.URL.RawQuery) {
		finilizeMultipartUpload(w, r, bucketPath, objectKey)
		return
	}

	s3error(w, r, "Malformed request", "InvalidArgument", http.StatusNotFound)

}

func handleListBuckets(w http.ResponseWriter, r *http.Request) {
	// List bucket directories
	files, err := os.ReadDir(bucketPath)
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

func isMultiPartUpload(r *http.Request) (error, bool, string, string) {

	// Parse the URI
	parsedURL, err := url.Parse(r.URL.RequestURI())
	if err != nil {
		fmt.Println("Error parsing URI:", err)
		return err, false, "", ""
	}

	// Get the query parameters
	queryParams := parsedURL.Query()

	uploadId := queryParams.Get("uploadId")
	partNumber := queryParams.Get("partNumber")

	var ret bool = false

	if uploadId != "" {
		ret = true
	}

	return nil, ret, uploadId, partNumber
}

func extractBucketAndKey(r *http.Request) (string, string, map[string]string) {
	query := r.URL.RawQuery

	parts := strings.SplitN(r.URL.Path[1:], "/", 2)
	bucket := parts[0]
	key := ""
	if len(parts) > 1 {
		key = parts[1]
	}

	params := make(map[string]string)

	if key == "" && query != "" {
		tokens := strings.Split(query, "&")

		for _, arg := range tokens {
			//fmt.Println(arg)
			p := strings.Split(arg, "=")
			params[p[0]] = p[1]
		}
		key = strings.Replace(params["prefix"], params["delimiter"], "/", -1)

	}

	return bucket, key, params
}
