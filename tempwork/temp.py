#!/usr/bin/python

import warc
from cStringIO import StringIO #for converting payloads into binary strings
# that reside in-memory rather than on-file
import os #for file length
from subprocess import call #for executing ffmpeg
import shlex #for properly parsing command lines for insertion into call()s

# f will hold array of records?
# No. f holds a WARCFile object, and has an iterator that invokes a
# WARCReader object, which in turn invokes a WARCHeader object (and WARCRecord
# object?), which is ultiamtely what we want to look at to identify records.
#f = warc.open("twitchtv-393d967f1c17b787e5f0756907b6e514621f16c2-20140815-115402.warc.gz")
#f = warc.open("test.warc.gz")
#f = warc.open("cloned.warc.gz")
#f = warc.open("wtf.warc.gz")
f = warc.open("twitchtv-8bbd60023627ec4a666c026a38b0b587bcf9fcb3-20140817-233758.warc.gz")

#print "f is: ", f

newfile = warc.open("truncated.warc.gz", "w")

#print "newfile is: ", newfile

# each record is an object, and each object has a header, which is a dictionary.
# some keys are mandatory and guaranteed to be there (warc-type, content-type,
# content-length). Others, like "warc-target-uri", might not be, so need to
# make sure it exists before asking for it.
for record in f:
    print "WARC-Type: ", record['warc-type']
    print "WARC-Content-Type: ", record['content-type']
    print "Content-Length: ", record['Content-Length']
    
    for key in record.header.keys():
#        print key, ": ",record[key]
        if key == "warc-target-uri":
            print "WARC-Target-URI:", record['warc-target-uri']
        if key == "warc-identified-payload-type":
            print "WARC-Identified-Payload-Type: ", record['warc-identified-payload-type']

# grab the payload
    if long(record['Content-Length']) >= 500000:

        #add truncated header
        print "Adding warc-truncated header."
        record['warc-truncated'] = "length"

        #extract the payload
        print "Time to grab the payload."
        tempfile = open("intermediate.int", 'wb')
        print "Writing to intermediate.int"
        for line in record.payload:
            tempfile.write(line)
        print "Done writing to intermediate.int"   
        tempfile.close()
 
        # put the payload back
        tempfile2 = open("intermediate.int", 'rb')
        data = tempfile2.read()
        stream = StringIO(data)
        tempfile2.close()
        stream.seek(0, os.SEEK_END)
        thelength = stream.tell()
        stream.seek(0)
        record.payload = warc.utils.FilePart(fileobj=stream,length=thelength)
        #stream.close()
        # "IO Operation on a closed file"

# change headers
#    temp = record['content-type']
#    record['content-type'] = "lolwutermelon"
#    print "WARC-Content-Type: ", record['content-type']        
#    record['content-type'] = temp
#    print "WARC-Content-Type: ", record['content-type']

# won't work to get payload
# resulted in "grab the payload" above
#    print "Record's payload is: ", record.payload
#    print "Record is: ", record
#    for j in record:
#        print "j is: ", j

# copy-paste records to new file

    new_header = warc.WARCHeader(record.header)

    if long(record['content-length']) < 500000:

        print "Copying payload..."
        new_payload = StringIO()
        for line in record.payload:
            new_payload.write(line)
        #if we don't seek back to 0, new_payload.read() is empty
        new_payload.seek(0)
        print "Done copying payload."

    else:
        print "Found long content-length. Truncating..."
        new_payload = StringIO()
        decrement = 25
        #Grab some lines
        print "Gonna grab some lines. Decrement: ", decrement
        for line in record.payload:
            print "Grabbing a line."
            new_payload.write(line)
            decrement -= 1
            print "Decrement: ", decrement
            if decrement == 0:
                break
        new_payload.seek(0)
        tempfile.close()
        print "Done truncating."

    # set defaults to false so that the warc library doesn't add headers
    # (incidentally, wget-lua probably *should* be setting the
    # warc-payload-digest header, but that is neither here nor there at the
    # moment)
    new_rec = warc.WARCRecord(payload=new_payload.read(), headers=new_header, defaults=False)

    print "Copying record to new .warc.gz"
    newfile.write_record(new_rec)
    print "Done copying record to new .warc.gz"

    print "\n\n"

# Should probably delete old warc at this point, since new warcfile has all
# of the old records, and we've already got another copy of the main
# payload. If we proceed to write out the full newfile with the shrunken
# payload before deleting the old warc, we'll basically be using nearly
# 3x the interim diskspace rather than 2x. (Don't get me wrong, I'd love
# to have more of a generator-like setup that negates the need to use
# twice the disk space, but it's beyond the scope of my abilities at the
# moment and I don't think I'd be able to get up to speed before the
# deadline for this project drops (August 27 2014))

# Now, we need to convert the flv, and add conversion records

# Our "payload.flv" is not quite an flv yet; the payload still includes the
# HTTP Response headers. We need to grep for "CRLFCRLF" and then chop off
# anything prior to it, including it, leaving nothing but the flv file for
# ffmpeg to work with.

thefile = open("intermediate.int").read() # NOT A FILE; just a "str"
theflv = thefile.split('\r\n\r\n')[1]

writetheflv = open("samplethis.flv", "w")
writetheflv.write(theflv)

writetheflv.close()

# Start sampling the video.

print "Getting snapshots."

# snapshot
# This is the "proper" way to handle complex command lines with lots of args
# https://stackoverflow.com/questions/8581140/python-subprocess-call-with-arguments-having-multiple-quotations
ffmpegsnapshotargs = shlex.split("ffmpeg -i samplethis.flv -vf fps=fps=1/15 -f image2 -q:v 1 images%05d.jpg")
call(ffmpegsnapshotargs)

print "Compressing snapshots."

# compress all the snapshots
tarargs = shlex.split("tar -czvf snapshots.tar.gz *.jpg")
call(tarargs)

print "Shrinking. This is gonna take a while."

# shrink; using the webm format at this resolution cuts the file size by
# *about* an order of magnitude, while still maintaining more-or-less
# perfectly crisp detail and motion. I'm thinking we don't need to drop
# frames, and that cutting the resolution down to this ~240P-level
# resolution is good enough.
ffmpegshrinkargs = shlex.split("ffmpeg -i samplethis.flv -c:v libvpx -b:v 500K -c:a libvorbis -s 432x243 shrunken-to-webm.webm")
call(ffmpegshrinkargs)

# The final size of snapshots and shrunken video is anywhere from a fifth to
# a seventh of the original file size.

print "Removing intermediate files now that final sampled outputs are ready."

# remove original file intermediates: "intermediate.int" and "samplethis.flv"
rmargs = shlex.split("rm intermediate.int samplethis.flv *.jpg")
call(rmargs)

# add the gunzip'd snapshots and shrunken video as WARCRecords to the newer
# WARCFILE



# All done

stream.close() # close stream if it was opened (can't close it prior to closing
               # warcfile because FilePart in the warc library continues to use
               # the stream; but wouldn't it fall out of scope anyway outside
               # of the for-loop?)
newfile.close()# The new warcfile is finished.

raise SystemExit
