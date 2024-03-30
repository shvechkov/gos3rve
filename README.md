# gos3rve

### Serve files off your local HDD via S3 API

Use this utility to expose local (DAS) storage via S3 compatible API 

Example:
```
./gos3rve  -p 8080 -user_id you@email.com -key_id s3KeyID -key_val s3SecretKey
```

Get full list of flags/usage by running -h flag:

```
Usage of ./gos3rve:
  -config string
    	configuration file  (default "./config.xml")
  -dir_buckets string
    	dir to store buckets (default "./buckets/")
  -dir_uploads string
    	temp dir to store upload parts (default "./uploads/")
  -help
    	Show usage
  -key_id string
    	Access Key ID (default "muB07ZERr4")
  -key_val string
    	Secret Access Key (default "U8J89Z6XZCwXBWv1lP8tbzK35AaiR7Fz")
  -p int
    	Port to listen on (default 8080)
  -region string
    	S3 region (default "us-east-1")
  -user_id string
    	AWS S3 user ID (default "c5dbe9e2-4d44-404a-96f9-bd1dc1163a4a")
  -user_name string
    	AWS S3 user name (default "s3user@amazon.com")
```

#### supported S3 operations 

tested with s3cmd 

| AWS API  | supported | s3cmdcmd |
|:------|:-------:|----------:|
| ListObjectsV2 | yes |  ls|
| CreateBucket | yes |  mb |
| DeleteBucket | yes|  rb|
| DeleteBucket | yes|  rb|
| PutObject | yes | put |
| GetObject | yes | get |
| DeleteObject | yes | del|




TBD 
- implement mv /renames 

- parts of multipart uploads should go into separate temp dir (to prevent end user from seeing partially uploaded objects/to maintain atomicity). If multipart-part upload fails we should clean stale parts .. This can be done asynchronously by GC thread which wil monitor temp uploads dir

