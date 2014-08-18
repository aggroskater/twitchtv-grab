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
    header = warc.WARCHeader(existing_record.header)
    record = warc.WARCRecord(header, existing_record.payload)
    f.write_record(record)

But I've got family functions to attend. So, later.

--------

If someWARCRecordObject.payload is going to be of type FilePart, then
why would other parts of the library expect strings? We can use str()
to stringify the object, but sheesh. I'm probably still missing something
here.

Inded. I am missing something. Because stringifying the instance just
spits out a string describing the object:

    <warc.utils.FilePart instance at 0x7f5b8b169710>

Not helpful at all.

--------

Now I'm stuck at a point where I'm able to more-or-less copy WARCRecord
objects into new files, but not perfectly. The objects seem to match, but
somewhere in the process, I suspect some extra whitespace, or newlines,
or carriage returns, or *something* is getting added in, making the copied
.warc.gz slightly different from the source.

-------

![FFFFFFFUUUUUUUUUUUU](https://i.imgur.com/ALXK2wY.png "FFFFFFFUUUUUUUUUUUU")

So, it appears when compressing with gunzip with multiple members, each member
is delimited by an instance of the title of the collection. I.e., a
"collection.gz" file with multiple members will have "collection.gz" as a
string in the binary of the file (probably along with some kind of iterator)
prior to each member. My initial file was "test.warc.gz". My new file was
"cloned.warc.gz". The "test.warc.gz" had five members in it (WARCFiles).
Guess how many bytes bigger the "cloned.warc.gz" file was than the
"test.warc.gz" file:

    10 bytes

Yep. A cursory glance in a hex editor, and all of the records basically look
identical.

However, now I can't use crypto digests to verify that we have a genuine
copy (unless, maybe, I rename the source file prior to acting on it,
and give the new file the same name? That sounds positively braindead.)

In any case, I'm getting relatively comfortable with the warc python
library.

---------

That's enough for tonight.

--------

I suspect this is why, when I extract the payload to a separate file,
the payload appears to have "disappeared" when I attempt to simply
truncate it later in the code.

https://stackoverflow.com/questions/231767/what-does-the-yield-keyword-do-in-python

The pesky "yield" keyword.
