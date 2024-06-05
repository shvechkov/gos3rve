# [gos3rve](https://github.com/shvechkov/gos3rve)

### Expose local file system via S3 API ( S3 Proxy/connector/server ) 

TLDR;
```
git clone git@github.com:shvechkov/gos3rve.git && cd gos3rve && go build 

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

Configuration values can be passed in configuration file (using --config flag)
Here is the sample config file: 

```
<root>
    <AccessKeyId>any</AccessKeyId>
    <SecretAccessKey>key</SecretAccessKey>
    <Region>us-east-1</Region>
    <UploadsPath>./uploads</UploadsPath>
    <BucketsPath>./buckets</BucketsPath>
    <Port>8080</Port>
</root>
```


### supported S3 operations 

tested with s3cmd 

| AWS API  | supported | s3cmd |
|:------|:-------:|----------:|
| ListObjectsV2 | yes |  ls|
| CreateBucket | yes |  mb |
| DeleteBucket | yes|  rb|
| PutObject | yes | put |
| GetObject | yes | get |
| DeleteObject | yes | del|


### How to build 
Install golang on your platform and execute :
```
git clone git@github.com:shvechkov/gos3rve.git && cd gos3rve && go build 
```

For creating static executable (which runs on all Linux platforms w. same arch) run:
```
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -tags netgo -ldflags '-w'
```

### Run as a service

```
sudo cp gos3rve.service  /etc/systemd/system/
sudo systemctl start gos3rve
sudo systemctl enable gos3rve
```


### TBD 
- parts of multipart uploads should go into separate temp dir (to prevent end user from seeing partially uploaded objects/to maintain atomicity). If multipart-part upload fails we should clean stale parts .. This can be done asynchronously by GC thread which wil monitor temp uploads dir
- implement mv /renames, implement other missing APIs (?)
- add option to allow only unique uploads and/or use FS reflinks, if supported ( xfs/btrs/zfs), to clone existing object instead of creating new onces    



### Disclaimer

This is a toy project - quick and dirty code created for testing and educational purposes only. Use at your own risk.
Look at [ceph](https://github.com/ceph/ceph) or [seaweedfs](https://github.com/seaweedfs/seaweedfs/tree/master) if you need production grade scalable solution



