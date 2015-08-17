####Motivation:
s3cmd and aws-cli tools do not calculate etag value for your download.  If either errors out, you will have a partially truncated file and possibly something in stderr.  If you’re running large jobs (i.e., accessing 10s of thousands of objects) and you’re accessing them as a stream (i.e., everything in memory),  it will be easy to miss these errors.  This script calculates etag and checksum on the file stream directly, producing .etag and .md5 files for the s3 object. It also compares the calculated etag against Amazon’s etag for ensuring consistency.

####Features: 
- makes requests of s3 API in chunks, with the chunksize based on the number of parts the object has (typically 15 Mb each), 
- hashes each chunk in order to reproduce the amazon etag value,
- hashes the full stream to generate the file’s md5 checksum,
- downloads the file with (-o, --output-file), 
- streams the file (with - or –stdin).

####Requirements:
- Besides needing some extra python modules (boto, progressbar, -- the rest come with Python I think).
- You will need a .boto file in your home directory, with your aws keys.

####Could use some additional improvements:
#####FIXME:
- Fix retry mechanism for handling errors when making requests of the API
