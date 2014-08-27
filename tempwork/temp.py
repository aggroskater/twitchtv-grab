#!/usr/bin/python

import warc
from cStringIO import StringIO # for converting payloads into binary strings
                               # that reside in-memory rather than on-file
import os # for file length, setting environment variables
from subprocess import call # for executing ffmpeg
import shlex # for properly parsing command lines for insertion into call()s
import glob # for grabbing all *.jpg files

# f will hold array of records?
# No. f holds a WARCFile object, and has an iterator that invokes a
# WARCReader object, which in turn invokes a WARCHeader object (and WARCRecord
# object?), which is ultimately what we want to look at to identify records.
f = warc.open("twitchtv-8bbd60023627ec4a666c026a38b0b587bcf9fcb3-20140817-233758.warc.gz")

# If you open a new warcfile with warc.open, and of a filename ending in
# "*.gz", then the warc library will handle writing the compressed data
# to the gzip member files for you transparently.
newfile = warc.open("truncated.warc.gz","w")

# a few headers we need to grab for use in new records we will write in the
# new warc file:
#
# * From the warcinfo record: WARC-Record-ID
#
#   This will be utilized by resource records. These resource records will
#   have their "WARC-Warcinfo-ID" headers set to this value.
# 
# * From the metadata record: WARC-Record-ID
#
#   This will be utilized in the additional resource records we write. (The 
#   resource records will contain logs from ffmpeg). These records will have
#   their "WARC-Concurrent-To" header set to this value.
#
#   NOTE: WARC files are not constricted to a single metadata record. So, the
#   phrase "the metadata record" is specific to this particular grab, which
#   establishes a single metadata record, links it to the warcinfo record, and
#   makes all other "metadata" (like logs) stay in separate "resource records"
#   that are concurrent to this metadata record. A good practice, IMHO, but
#   it's not mandated by the standard.
#
# * From the truncated response record: WARC-Record-ID
#
#   This will be set as the "WARC-Refers-To" header in the conversion records.
warcinfo_record_ID = None
metadata_record_ID = None 
truncated_record_ID = None 

# each record is an object, and each object has a header, which is a dictionary.
# some keys are mandatory and guaranteed to be there (warc-type, content-type,
# content-length). Others, like "warc-target-uri", might not be, so need to
# make sure it exists before asking for it.
for record in f:

#    print "WARC-Type: ", record['warc-type']
#    print "WARC-Content-Type: ", record['content-type']
#    print "Content-Length: ", record['Content-Length']
    
#    for key in record.header.keys():
#        print key, ": ",record[key]
#        if key == "warc-target-uri":
#            print "WARC-Target-URI:", record['warc-target-uri']
#        if key == "warc-identified-payload-type":
#            print "WARC-Identified-Payload-Type: ", record['warc-identified-payload-type']

# grab the lengthy payload (that probably contains the video)
    if long(record['Content-Length']) >= 500000:

        # grab the truncated record id (will use in later conversion records)
        truncated_record_ID = record['warc-record-id']

        #add truncated header
#        print "Adding warc-truncated header."
        record['warc-truncated'] = "length"

        #extract the payload
#        print "Time to grab the payload."
        tempfile = open("intermediate.int", 'wb')
#        print "Writing to intermediate.int"
        for line in record.payload:
            tempfile.write(line)
#        print "Done writing to intermediate.int"   
        tempfile.close()
 
        # put the payload back; iterating through record.payload
        # invokes a generator on the payload that seems to
        # "eat up" the payload in the original file. I say so because
        # attempting to, say, write the payload out twice (to TWO files)
        # will fail, as will any attempt to read out the payload again
        # without first "putting it back." (I'd love an explanation for
        # just what's going on here; but for now, this hack works)
        # (for the record with the long content-length, we end up reading
        # the payload twice; once here, to get it to a separate file, and
        # once again, in COPY PAYLOAD, to write out a truncated version to
        # the new warc file)
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

# adjust the warcinfo record to include additional information and grab its
# warc-record-id
    elif (record['WARC-Type'] == "warcinfo"):

        # grab the record-id for later use in resource records
        warcinfo_record_ID = record['warc-record-id']

        # gotta add another "software" key to the content-block of the
        # warcinfo record that indicates the use of ffmpeg.

        warcinfo_stream = StringIO()
        for line in record.payload:
            warcinfo_stream.write(line)
        # trailing \r\n\r\n is already present in the payload; just seek back
        # two bytes (yes, the second \r\n will get clobbered; potential unicode
        # byte-length issues here) and then tack on the additional lines you
        # need to like so:
        warcinfo_stream.seek(-2,os.SEEK_END)
        warcinfo_stream.write("software: ffmpeg/2.3.1\r\n\r\n")
        warcinfo_stream.seek(0, os.SEEK_END)
        warcinfo_stream_len = warcinfo_stream.tell()
        warcinfo_stream.seek(0)
        record.payload = warc.utils.FilePart(fileobj=warcinfo_stream,length=warcinfo_stream_len)

# grab the metadata record's warc-record-id for later resource records
    elif (record['WARC-Type'] == "metadata"):
        metadata_record_ID = record['warc-record-id']

# BEGIN CREATING NEW RECORD

    # COPY HEADER
    # Should we add defaults=False ? It seems that some additional headers
    # are added in WARCHeader as well as WARCRecord. However, they don't
    # seem harmful: digests and timestamps.
    new_header = warc.WARCHeader(record.header)

    # COPY PAYLOAD

    # if the current record gets truncated, then set the content-length
    # to the new, truncated length as per spec.
    truncated_flag = None

    # short record payloads
    if long(record['content-length']) < 500000:

#        print "Copying payload..."
        new_payload = StringIO()
        for line in record.payload:
            new_payload.write(line)
        #if we don't seek back to 0, new_payload.read() is empty
        new_payload.seek(0)
#        print "Done copying payload."

    # long record payloads (the one that probably has video data)
    else:
#        print "Found long content-length. Truncating..."
        new_payload = StringIO()
        decrement = 25
        #Grab some lines
#        print "Gonna grab some lines. Decrement: ", decrement
        for line in record.payload:
#            print "Grabbing a line."
            new_payload.write(line)
            decrement -= 1
#            print "Decrement: ", decrement
            if decrement == 0:
                break
        # be kind: rewind
        new_payload.seek(0)
        truncated_flag = True

        # not sure what this was doing here.
        #tempfile.close()
#        print "Done truncating."

    # set defaults to false so that the warc library doesn't add headers
    # (incidentally, wget-lua probably *should* be setting the
    # warc-payload-digest header, but that is neither here nor there at the
    # moment)

    # CREATE RECORD FROM HEADER AND PAYLOAD

    new_rec = warc.WARCRecord(payload=new_payload.read(), headers=new_header, defaults=False)

    if truncated_flag:

#        print "Adjusting content-length header"

        # From page 9 of the ISO WARC Standard:
        #
        # "The WARC-Truncated field may be used on any WARC record. The WARC
        # field Content-Length shall still report the actual truncated size of
        # the record block."

        # Get the length of the truncated content-block and set
        # Content-Length header appropriately
        new_payload.seek(0)
        new_payload.seek(0, os.SEEK_END)
        thelength = new_payload.tell()
        new_rec['content-length'] = str(thelength)
        new_payload.seek(0)

    # WRITE THE NEW RECORD OUT TO THE NEW WARCFILE
    # (the warc library handles the gz-compression and putting each record
    # in a separate gz "member" transparently; no need to much with the gzip
    # library ourselves)
 
#    print "Copying record to new .warc.gz"
    newfile.write_record(new_rec)
#    print "Done copying record to new .warc.gz"

    print "\n\n"

# END OF MAIN FOR LOOP; at this point, we have a new warcfile with
# copied and truncated records; now, we need to sample the content and add
# these "conversion" records to the warc file.

# Should probably delete old warc at this point, since new warcfile has all
# of the old records, and we've already got another copy of the main
# payload. If we proceed to write out the full newfile with the shrunken
# payload before deleting the old warc, we'll basically be using nearly
# 3x the interim diskspace rather than 2x. (Don't get me wrong, I'd love
# to have more of a generator-like setup that negates the need to use
# twice the disk space, but it's beyond the scope of my abilities at the
# moment and I don't think I'd be able to get up to speed before the
# deadline for this project drops (August 27 2014) Update: LOL Twitch is
# already deleting things on August 26; oh well, I suppose this code
# could come in handy if the IA suddenly needs to compress lots of
# material)

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

print "********************* \n\n Getting snapshots. \n\n*********************"

os.environ["FFREPORT"] = "file=ffmpeg-snapshots.log"

# snapshot
# This is the "proper" way to handle complex command lines with lots of args
# https://stackoverflow.com/questions/8581140/python-subprocess-call-with-arguments-having-multiple-quotations
ffmpegsnapshotargs = shlex.split("ffmpeg -i samplethis.flv -vf fps=fps=1/15 -f image2 -q:v 1 images%05d.jpg")
call(ffmpegsnapshotargs)

print "********************* \n\n Compressing snapshots. \n\n*********************"

imagelist = glob.glob("*.jpg")
imageliststring = ' '.join(imagelist)
tarcommand = "tar -czvf snapshots.tar.gz " + imageliststring

# compress all the snapshots
tarargs = shlex.split(tarcommand)
call(tarargs)

# delete jpgs
rmcommand = "rm " + imageliststring
rmargs = shlex.split(rmcommand)
call(rmargs)

print "********************* \n\n Shrinking Video. (This will take a while) \n\n*********************"

os.environ["FFREPORT"] = "file=ffmpeg-shrinking.log"

# shrink; using the webm format at this resolution cuts the file size by
# *about* an order of magnitude, while still maintaining more-or-less
# perfectly crisp detail and motion. I'm thinking we don't need to drop
# frames, and that cutting the resolution down to this ~240P-level
# resolution is good enough.

# We really need to check for resolution and select an output resolution
# appropriately; this one-liner only works for 16:9 inputs

ffmpegshrinkargs = shlex.split("ffmpeg -i samplethis.flv -c:v libvpx -b:v 500K -c:a libvorbis -s 432x243 shrunken-to-webm.webm")
call(ffmpegshrinkargs)

# The final size of snapshots and shrunken video is anywhere from a fifth to
# a seventh of the original file size.

print "********************* \n\n Removing temporary files \n\n*********************"

# remove original file intermediates: "intermediate.int" and "samplethis.flv"
rmargs = shlex.split("rm intermediate.int samplethis.flv")
call(rmargs)

os.environ["FFREPORT"] = ""

# add the gunzip'd snapshots and shrunken video as WARCRecords to the newer
# WARCFILE
#
# According to spec, the Content-Length of a content-block in a gzipped
# warcfile corresponds to the COMPRESSED LENGTH, not the uncompressed length.
# So we'll need to get that compressed length once we've added the record,
# and set the Content-Length header appropriately.

# Add ffmpeg log records
ffmpegsampleheader = warc.WARCHeader({
    "WARC-Type": "resource",
    "WARC-Warcinfo-ID": warcinfo_record_ID,
    "Content-Type": "text/plain",
    "WARC-Concurrent-To": metadata_record_ID
})
ffmpegsamplepayload = StringIO(open("ffmpeg-snapshots.log").read()).getvalue()
ffmpegsamplerecord = warc.WARCRecord(headers=ffmpegsampleheader,payload=ffmpegsamplepayload)
newfile.write_record(ffmpegsamplerecord)

ffmpegshrinkheader = warc.WARCHeader({
    "WARC-Type": "resource",
    "WARC-Warcinfo-ID": warcinfo_record_ID,
    "Content-Type": "text/plain",
    "WARC-Concurrent-To": metadata_record_ID
})
ffmpegshrinkpayload = StringIO(open("ffmpeg-shrinking.log").read()).getvalue()
ffmpegshrinkrecord = warc.WARCRecord(headers=ffmpegshrinkheader,payload=ffmpegshrinkpayload)
newfile.write_record(ffmpegshrinkrecord)

# remove ffmpeg logs
call(shlex.split("rm ffmpeg-snapshots.log ffmpeg-shrinking.log"))

# Add ffmpeg snapshots record
snapshotrecord = warc.WARCRecord(
    headers = warc.WARCHeader({
        "WARC-Type": "conversion",
        "Content-Type": "application/x-gtar",
        "WARC-Refers-To": truncated_record_ID
    }),
    payload = StringIO(open("snapshots.tar.gz").read()).getvalue()
)
newfile.write_record(snapshotrecord)

# remove snapshots
call(shlex.split("rm snapshots.tar.gz"))

# Add ffmpeg shrunken record
shrinkrecord = warc.WARCRecord(
    headers = warc.WARCHeader({
        "WARC-Type": "conversion",
        "Content-Type": "video/webm",
        "WARC-Refers-To": truncated_record_ID
    }),
    payload = StringIO(open("shrunken-to-webm.webm").read()).getvalue()
)
newfile.write_record(shrinkrecord)

# remove shrunken webm.
# call(shlex.split("rm shrunken-to-webm.webm")

# All done

stream.close() # close stream if it was opened (can't close it prior to closing
               # warcfile because FilePart in the warc library continues to use
               # the stream; but wouldn't it fall out of scope anyway outside
               # of the for-loop? LOLNOPE. Python doesn't do block-level
               # scoping)
newfile.close()# The new warcfile is finished.

raise SystemExit
