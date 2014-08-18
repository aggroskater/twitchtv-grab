#!/usr/bin/python

import warc
from cStringIO import StringIO #for converting payloads into binary strings
# that reside in-memory rather than on-file

# f will hold array of records?
# No. f holds a WARCFile object, and has an iterator that invokes a
# WARCReader object, which in turn invokes a WARCHeader object (and WARCRecord
# object?), which is ultiamtely what we want to look at to identify records.
#f = warc.open("twitchtv-393d967f1c17b787e5f0756907b6e514621f16c2-20140815-115402.warc.gz")
#f = warc.open("test.warc.gz")
f = warc.open("cloned.warc.gz")

#print "f is: ", f

#newfile = warc.open("cloned.warc.gz", "w")

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
        print key, ": ",record[key]
        if key == "warc-target-uri":
            print "WARC-Target-URI:", record['warc-target-uri']
        if key == "warc-identified-payload-type":
            print "WARC-Identified-Payload-Type: ", record['warc-identified-payload-type']

# grab the payload
#    if record['Content-Length'] == "706332742":
#        print "Time to grab the payload."
#        file = open("temp_from_gz.flv", 'wb')
#        print "Writing to temp_from_gz.flv"
#        for line in record.payload:
#            file.write(line)
#        print "Done writing to temp_from_gz.flv"    

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

#    new_header = warc.WARCHeader(record.header)

#    print "Copying payload..."
#    new_payload = StringIO();
#    for line in record.payload:
#        new_payload.write(line)
    #if we don't seek back to 0, new_payload.read() is empty
#    new_payload.seek(0)
#    print "Done copying payload."

    # set defaults to false so that the warc library doesn't add headers
    # (incidentally, wget-lua probably *should* be setting the
    # warc-payload-digest header, but that is neither here nor there at the
    # moment)
#    new_rec = warc.WARCRecord(payload=new_payload.read(), headers=new_header, defaults=False)

#    print "Copying record to new .warc.gz"
#    newfile.write_record(new_rec)
#    print "Done copying record to new .warc.gz"

    print "\n\n"

#newfile.close()

raise SystemExit
