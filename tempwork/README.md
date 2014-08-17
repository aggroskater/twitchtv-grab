Writing WARC records
====================

According to the documentation, writing a warc record to a file should be as
simple as:

    import warc
    f = warc.open("output.warc.gz", "w")
    f.write_record(warc_record1)
    f.write_record(warc_record2)
    f.close()

Where presumably, warc_record instances can be grabbed and written like so:

    import warc
    f = warc.open("output.warc.gz", "w")
    g = warc.open("input.warc.gz")
    for record in g:
        f.write_record(record)

The idea is just to copy a .warc.gz file.

However, record isn't actually an instance of WARCRecord; it's
an instance of FilePart. And if you try to do this, the library crashes.
I've patched the relevant section of the WARCRecord class in warc.py to look
like this:

    def write_to(self, f)
        self.header.write_to(f)
        #f.write(self.payload)
        for line in self.payload:
            f.write(line)
        f.write("\r\n")
        f.write("\r\n")
        f.flush()

However, I don't like that. I think I'm doing something stupidly wrong. So
I'm going to experiment with something like this:

    import warc
    f = warc.open("output.warc.gz", "w")
    header = warc.WARCHeader(existing_record.headers)
    record = warc.WARCRecord(header, existing_record.payload)
    f.write_record(record)

But I've got family functions to attend. So, later.
