#!/usr/bin/python

import warc
from cStringIO import StringIO #for converting payloads into binary strings
# that reside in-memory rather than on-file
import os #for file length, setting environment variables
from subprocess import call #for executing ffmpeg
import shlex #for properly parsing command lines for insertion into call()s
import glob #for grabbing all *.jpg files

ffmpegsampleheader = warc.WARCHeader({
    "WARC-Type": "resource",
    "Content-Type": "text/plain",
    "WARC-Concurrent-To": "<urn:uuid:a8c7ef3d-16c5-476a-b35d-241ea429226c>"
})

#ffmpeglogreader = open("ffmpeg-snapshots.log")
#ffmpegpayload = StringIO(ffmpeglogreader.read()).getvalue()
#ffmpeglogreader.close()
#ffmpeglogreaderlength = os.stat("ffmpeg-snapshots.log").st_size

ffmpegpayload = StringIO(open("ffmpeg-snapshots.log").read()).getvalue()

#ffmpegsamplepayload = warc.utils.FilePart(fileobj=ffmpeglogreader,length=ffmpeglogreaderlength)

ffmpegsamplerecord = warc.WARCRecord(headers=ffmpegsampleheader,payload=ffmpegpayload)

test = warc.open("new-warc.warc.gz", "w")
test.write_record(ffmpegsamplerecord)
test.close()

print "Done."
