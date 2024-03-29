package main

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
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

	"github.com/google/uuid"
)

var (
	bucketPath   string
	uploadsPath  string
	cfgPath      string
	s3user       string
	userId       string
	s3region     string
	storageClass string
	keyId        string
	secretKey    string
	svcPort      int64
	help         bool
)

type BucketListResponse struct {
	Buckets []string `json:"buckets"`
}

type Config struct {
	XMLName         xml.Name `xml:"root"`
	AccessKeyId     string   `xml:"AccessKeyId"`
	SecretAccessKey string   `xml:"SecretAccessKey"`
	Region          string   `xml:"Region"`
	Port            int      `xml:"Port"`
	UploadsPath     string   `xml:"UploadsPath"`
	BucketsPath     string   `xml:"BucketsPath"`
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	log.Printf("Reading configuration from %s...\n", cfgPath)

	// Open the XML file
	file, err := os.Open(path)
	if err != nil {
		log.Printf("Error opening config file %s , err: %s\n", path, err)
		return cfg, err
	}
	defer file.Close()

	// Create a new decoder
	decoder := xml.NewDecoder(file)

	// Decode the XML data into the Person struct
	err = decoder.Decode(&cfg)
	if err != nil {
		log.Printf("Error parsing config file (%s) : %s \n", path, err)
		return cfg, err
	}

	return cfg, nil
}

func isFlagOn(name string) bool {
	for _, str := range os.Args {
		if str == "-"+name {
			return true
		}
	}
	return false
}

func main() {

	// Define flags with default values and descriptions
	flag.Int64Var(&svcPort, "p", 8080, "Port to listen on")
	flag.StringVar(&uploadsPath, "dir_uploads", "./uploads/", "temp dir to store upload parts")
	flag.StringVar(&bucketPath, "dir_buckets", "./buckets/", "dir to store buckets")
	flag.StringVar(&s3user, "user_name", "s3user@amazon.com", "AWS S3 user name")
	flag.StringVar(&userId, "user_id", uuid.New().String(), "AWS S3 user ID")
	flag.StringVar(&keyId, "key_id", genBase64Str(10), "Access Key ID")
	flag.StringVar(&secretKey, "key_val", genBase64Str(32), "Secret Access Key")
	flag.StringVar(&s3region, "region", "us-east-1", "S3 region")
	flag.StringVar(&cfgPath, "config", "./config.xml", "configuration file ")
	flag.BoolVar(&help, "h", false, "Show usage")

	flag.Parse()

	if help {
		flag.Usage()
		return
	}

	//first try load config .. Command line args override config values
	cfg, err := loadConfig(cfgPath)

	if err == nil {

		if cfg.AccessKeyId != "" && !isFlagOn("key_id") {
			keyId = cfg.AccessKeyId
		}

		if cfg.SecretAccessKey != "" && !isFlagOn("key_val") {
			secretKey = cfg.SecretAccessKey
		}

		if int64(cfg.Port) != 0 && !isFlagOn("p") {
			svcPort = int64(cfg.Port)
		}

		if cfg.UploadsPath != "" && !isFlagOn("dir_uploads") {
			uploadsPath = cfg.UploadsPath
		}

		if cfg.BucketsPath != "" && !isFlagOn("dir_buckets") {
			bucketPath = cfg.BucketsPath
		}

		if cfg.Region != "" && !isFlagOn("region") {
			s3region = cfg.Region
		}

		log.Printf("Loaded configuration from %s...", cfgPath)
		log.Printf("*** Note: command-line arguments take precedence over values from the configuration file")

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
	log.Printf("access key id  \"%s\" ...", keyId)

	err = http.ListenAndServe(":"+strconv.FormatInt(svcPort, 10), nil)
	log.Printf("Exitting (%s) \n", err.Error())
}

func handleRequest(w http.ResponseWriter, r *http.Request) {

	if res, err := authenticate(r); err != nil || !res {
		s3error(w, r, "Access Denied", "AccessDenied", http.StatusForbidden)
		return
	}

	switch r.Method {
	case http.MethodGet:
		handleGetRequest(w, r)
	case http.MethodPut:
		handlePutRequest(w, r)
	case http.MethodDelete:
		handleDeleteRequest(w, r)
	case http.MethodPost:
		handlePostRequest(w, r)
	case http.MethodHead:
		handleHeadRequest(w, r)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleHeadRequest(w http.ResponseWriter, r *http.Request) {
	// Extract bucket name and object key from URL
	bucketName, objectKey, params := extractBucketAndKey(r)

	if bucketName == "" {
		s3error(w, r, "The specified bucket is not valid.", "InvalidBucketName", http.StatusBadRequest)
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
		log.Printf("HEAD on dir %s \n", filePath)
		return
	}

	getObjectHead(w, r, filePath)

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

	putObject(w, r, filePath, strings.HasSuffix(objectKey, "/"))
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

		s := fmt.Sprintf(`
		<InitiateMultipartUploadResult>
			<Bucket>%s</Bucket>
			<Key>%s</Key>
			<UploadId>%s</UploadId>
		</InitiateMultipartUploadResult>
`, bucketName, EscapeStringForXML(objectKey), strconv.FormatInt(time.Now().UnixNano(), 10))

		var buffer bytes.Buffer
		buffer.WriteString(s)

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
