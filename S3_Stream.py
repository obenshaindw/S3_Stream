import os
import sys
import math
import time
import hashlib
import binascii
import re
import urlparse
import boto
import logging
import argparse
from progressbar import ProgressBar
from boto.s3.connection import OrdinaryCallingFormat
from multiprocessing import Pool
from boto.s3.key import Key

'''Acknowledgements:
   Used portions of https://github.com/mumrah/s3-multipart to build this script
'''

def set_args():
    parser = argparse.ArgumentParser(description="Get checksum of S3 object, and optionally download or stream it to stdout.",
        prog="s3-download")
    parser.add_argument("url", help="The S3 url to access")
    parser.add_argument("-o", "--output-file", help="Download the file", 
                        default=False, action="store_true")
    parser.add_argument("-p", "--part-size", help="Size of file parts in Mb, defaults to 15Mb",
                        type=int, default=15)
    parser.add_argument("-max", "--max-tries", help = "Number of times to retry a failed part.", default=5)
    parser.add_argument("-nb", "--no-progress", help= "Supress progress bar.", default=False, action="store_true")
    parser.add_argument("-", "--stdout", help="Output to stdout", default=False, action="store_true")
    return parser.parse_args()

def hashstream(streamObj, hasher):
    hasher.update(streamObj)
    return hasher

def get_stream_part(  min_byte, max_byte, received, current_tries=0 ):
    keyObj = key
    max_tries = args.max_tries
    try:
        #print('Getting Range: bytes %d-%d' % (min_byte, max_byte))
        #file_stream = keyObj.get_contents_as_string(headers={'Range': "bytes=%d-%d" % (min_byte, max_byte)})
        resp = s3_conn.make_request('GET', bucket=bucket.name, key=key.name,
                                    headers={'Range':"bytes=%d-%d" % (min_byte, max_byte)})
        file_stream = resp.read()
        if args.output_file or args.stdout:
            write_to_file (min_byte, max_byte, file_stream)
        return file_stream, received

    #TODO verify that exception block is actually retrying correctly. NOTE: boto 
    except Exception as err:
        if current_tries > max_tries:
            sys.stderr.write('ERROR: Exceeded max number of retries on %d-%d: Part %d' % (min_byte, max_byte, received))
        else:
            current_tries +=1
            sys.stderr.write("WARNING:Waiting before retry %d seconds." % current_tries*3)
            time.sleep(current_tries * 3)
            sys.stderr.write("WARNING:Retrying part %d range %d-%d" % (received, min_byte, max_byte))
            get_stream_part( min_byte, max_byte, received, current_tries)

def update_hasher (hasher, file_stream, part):
    full_hash = hasher['full_hash']
    full_hash.update(file_stream)
    hashed_part = hashstream(file_stream, hashlib.md5()).hexdigest()
    hasher['parts'][part-1] = hashed_part
    hasher.update({'full_hash': full_hash})

def compose_etag (hasher):
    hash_as_bytes = ""
    for part in hasher['parts']:
        #print ('Part is %s' % part)
        hash_as_bytes += binascii.unhexlify(part) 
    #hash_as_bytes = '\n'.join(binascii.unhexlify(part) for part in hasher['parts'])
    #print(hash_as_bytes)
    etag_hash = hashlib.md5()
    etag_hash.update(hash_as_bytes)
    return etag_hash.hexdigest()  

def write_to_file(min_byte, max_byte, stream):
    if args.stdout:
        sys.stdout.write(stream)
    if args.output_file:
        fd= os.open(os.path.basename(args.url), os.O_WRONLY)
        os.lseek(fd, min_byte, os.SEEK_SET)
        os.write(fd, stream)
        os.close(fd)

def get_contents(part_number, keyObj, conn, bucketObj, chunksize):
    if args.output_file:
        fd = os.open(os.path.basename(args.url), os.O_CREAT)
        os.close(fd)
    chunksize = chunksize * 1024 * 1024
    size = keyObj.size
    #print("Size: %d, Chunksize: %d" % (size, chunksize))
    if not args.stdout:
        print('Size of %d bytes' % size)
    etag = keyObj.etag
    etagsum = re.findall(r"([a-fA-F\d]{32})", etag)[0]
    if not args.stdout:
        print('Etag sum=%s' % etagsum)
    if size > chunksize:
        parts = int(re.findall(r"\-([0-9]+).$", etag)[0])
    else:
        parts =1
    etag = '%s-%d' % (etagsum, parts)
    #print('Parts =%d' % parts)
    min_byte = 0;
    max_byte = chunksize-1
    full_hash = hashlib.md5()
    hasher = {'full_hash': full_hash, 'parts': [None] * parts}
    received = 0
    if not args.stdout:
        print('Retreiving file in %d parts, of %d mb' % (parts, chunksize/(1024*1024)))
    
    if size > chunksize:
        pbar = ProgressBar()
        for received in pbar(range(1,parts)):
            file_stream, received = get_stream_part( min_byte, max_byte, received)
            update_hasher(hasher, file_stream, received)
            min_byte = max_byte+1
            max_byte = max_byte+chunksize
    
    max_byte = size-1
    file_stream, received = get_stream_part( min_byte, max_byte, parts)
    update_hasher(hasher, file_stream, parts)
    computed_etag = ""
    if size > chunksize:
      computed_etag += "%s-%d" % (compose_etag(hasher), parts)
    else:
      computed_etag = "%s-%d" % (hasher['full_hash'].hexdigest(), parts)
    if computed_etag != etag:
        sys.stderr.write('ERROR: s3 etag: %s does not match computed etag: %s\n' % (etag, computed_etag))
    else:
        full_md5 = hasher['full_hash'].hexdigest()
        if not args.stdout:
            print ('Final md5sum:%s' % full_md5)
            print ('Recalculated etag: %s' % (computed_etag))       
        md5out = open(os.path.basename(args.url)+'.md5', 'w')
        md5out.write('%s  %s' % (full_md5,os.path.basename(args.url))) 
        md5out.close()
        etagout = open(os.path.basename(args.url)+'.etag', 'w')
        etagout.write('%s  %s' % (computed_etag,os.path.basename(args.url)))
        etagout.close()

def get_connection (s3_path):

    # Split out the bucket and the key
    split_rs = urlparse.urlsplit(s3_path)
    if split_rs.scheme != "s3":
        raise ValueError("'%s' is not an S3 url" % src)
  
    s3 = boto.connect_s3()
    s3 = boto.connect_s3(calling_format=OrdinaryCallingFormat())
    bucket = s3.lookup(split_rs.netloc, validate=False)
    if bucket == None:
        raise ValueError("'%s' is not a valid bucket" % split_rs.netloc)
    key = bucket.get_key(split_rs.path)
    if key is None:
        raise ValueError("'%s' does not exist." % split_rs.path)
   
    return (s3, bucket, key)

if __name__ == "__main__":

    args = set_args()
    try:
        s3_conn, bucket, key = get_connection(args.url)
    
    except:
        sys.stderr.write('ERROR: Connection Error!')

    get_contents(None, key, bucket, s3_conn, args.part_size)

