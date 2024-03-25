# gos3rve


TBD 

- parts of multipart uploads should go into separate temp dir (to prevent end user from seeing partially uploaded objects/to maintain atomicity). If multipart-part upload fails we should clean stale parts .. This can be done asynchronously by GC thread which wil monitor temp uploads dir

